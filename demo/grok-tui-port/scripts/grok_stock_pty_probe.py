#!/usr/bin/env python3
"""Deterministic stock-Grok PTY and live-server environment proof.

The live phase runs ``run`` while the server is up, stops the exact listener
PID returned here, then runs ``cleanup`` against the saved JSON.  ``self-test``
exercises the evidence validators without a server or Grok binary.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import hashlib
import json
import os
import pty
import secrets
import select
import signal
import shlex
import stat
import struct
import subprocess
import sys
import termios
import threading
import time
import urllib.error
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import urlparse

from grok_probe_common import (
    PortableSocketPath,
    graphql_escape,
    graphql_query,
    poll_until_deadline,
    require,
    self_test_portable_socket_path,
)


class ProbeDeadlineExceeded(TimeoutError):
    pass


class ClientGuard:
    """Own every pager resource across deadline and cleanup boundaries."""

    def __init__(self) -> None:
        self.process: subprocess.Popen[bytes] | None = None
        self.master_fd: int | None = None
        self.slave_fd: int | None = None
        self.pager_socket: PortableSocketPath | None = None

    def emergency_kill(self) -> None:
        """Kill without waiting so an asynchronous deadline cannot orphan the pager."""
        process = self.process
        if process is None:
            return
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def cleanup(self) -> None:
        """Release owned resources after the hard deadline is disarmed."""
        slave_fd, self.slave_fd = self.slave_fd, None
        master_fd, self.master_fd = self.master_fd, None
        process, self.process = self.process, None
        pager_socket, self.pager_socket = self.pager_socket, None
        first_error: Exception | None = None

        def attempt(operation: Callable[[], None]) -> None:
            nonlocal first_error
            try:
                operation()
            except Exception as error:
                if first_error is None:
                    first_error = error

        def close_fd(fd: int) -> None:
            try:
                os.close(fd)
            except OSError as error:
                if error.errno != errno.EBADF:
                    raise

        if slave_fd is not None:
            attempt(lambda: close_fd(slave_fd))
        if process is not None and master_fd is not None:
            attempt(lambda: terminate_client(process, master_fd))
        process_alive = False
        if process is not None:
            try:
                process_alive = process.poll() is None
            except Exception as error:
                process_alive = True
                if first_error is None:
                    first_error = error
        if process is not None and process_alive:
            def kill() -> None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

            attempt(kill)
            attempt(lambda: process.wait(timeout=3))
        if pager_socket is not None:
            attempt(pager_socket.cleanup)
        if master_fd is not None:
            attempt(lambda: close_fd(master_fd))
        if first_error is not None:
            raise first_error


def raise_probe_deadline(timeout: float, emergency_kill: Callable[[], None]) -> None:
    emergency_kill()
    raise ProbeDeadlineExceeded(f"stock pager probe exceeded {timeout:g}s")


@contextlib.contextmanager
def blocked_signal(signum: signal.Signals) -> Iterator[None]:
    """Defer one asynchronous signal across an ownership transition."""
    previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signum})
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)


@contextlib.contextmanager
def hard_deadline(
    timeout: float, emergency_kill: Callable[[], None] = lambda: None
) -> Iterator[None]:
    """Bound the whole probe while leaving time for its deterministic teardown."""
    require(timeout > 0, "total probe timeout must be positive")
    previous_handler = signal.getsignal(signal.SIGALRM)

    def expire(_signum: int, _frame: Any) -> None:
        raise_probe_deadline(timeout, emergency_kill)

    signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        yield
    finally:
        try:
            signal.setitimer(signal.ITIMER_REAL, 0)
        finally:
            signal.signal(signal.SIGALRM, previous_handler)


def run_with_deadline_cleanup(
    timeout: float,
    guard: ClientGuard,
    work: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Run active work under the alarm, then clean only after it is disarmed."""
    try:
        with hard_deadline(timeout, guard.emergency_kill):
            return work()
    finally:
        guard.cleanup()


def validate_endpoint(
    expected: str,
    applied: str,
    suffixed_env: str | None,
    obsolete_env: str | None,
) -> None:
    require(expected != "", "expected endpoint is empty")
    require(applied == expected, "applied backend endpoint differs from the job")
    require(
        suffixed_env == expected,
        "GENTS_GROK_PORT_ENDPOINT_1 is absent or differs from the job",
    )
    require(
        obsolete_env is None,
        "obsolete GENTS_GROK_PORT_ENDPOINT must be unset",
    )


def validate_listener_identity(claimed_pid: int, actual_pids: list[int]) -> None:
    require(claimed_pid > 1, "listener PID must be positive")
    require(
        actual_pids == [claimed_pid],
        f"claimed listener PID {claimed_pid} does not uniquely own the port: {actual_pids}",
    )


def graphql_port(graphql_url: str) -> int:
    parsed = urlparse(graphql_url)
    require(parsed.scheme in {"http", "https"}, "live GraphQL URL must be HTTP(S)")
    return parsed.port or (443 if parsed.scheme == "https" else 80)


def backend_endpoint_from_data(data: dict[str, Any], backend_id: str) -> str:
    rows = data.get("InferenceBackend")
    require(isinstance(rows, list) and len(rows) == 1, "expected exactly one backend row")
    row = rows[0]
    require(isinstance(row, dict), "backend row is not an object")
    require(row.get("backend_id") == backend_id, "queried backend identity drifted")
    endpoint = row.get("endpoint")
    require(isinstance(endpoint, str) and endpoint, "queried backend endpoint is empty")
    return endpoint


def queried_backend_endpoint(graphql_url: str, backend_id: str) -> str:
    escaped = graphql_escape(backend_id)
    data = graphql_query(
        graphql_url,
        f'{{ InferenceBackend(filter: {{backend_id: {{_eq: "{escaped}"}}}}, limit: 2) '
        "{ backend_id endpoint } }",
    )
    return backend_endpoint_from_data(data, backend_id)


def request_rows_for_prompt(graphql_url: str, prompt: str) -> list[dict[str, Any]]:
    escaped = graphql_escape(prompt)
    data = graphql_query(
        graphql_url,
        f'{{ AgentRequest(filter: {{content: {{_eq: "{escaped}"}}, '
        'behavior_id: {_eq: "port-live"}}, order: {created_at: DESC}, limit: 2) '
        "{ request_id lifecycle_state behavior_id session_id content } }",
    )
    rows = data.get("AgentRequest")
    require(isinstance(rows, list), "AgentRequest query did not return a list")
    for row in rows:
        require(isinstance(row, dict), "AgentRequest row is not an object")
    return rows


def assistant_rows_for_request(
    graphql_url: str, request_id: str
) -> list[dict[str, Any]]:
    escaped = graphql_escape(request_id)
    data = graphql_query(
        graphql_url,
        f'{{ AgentMessage(filter: {{request_id: {{_eq: "{escaped}"}}}}, '
        "order: {sequence: ASC}, limit: 8) { request_id session_id sequence role content } }",
    )
    rows = data.get("AgentMessage")
    require(isinstance(rows, list), "AgentMessage query did not return a list")
    for row in rows:
        require(isinstance(row, dict), "AgentMessage row is not an object")
    return rows


def visible_assistant_text(content: Any) -> str:
    require(isinstance(content, str) and content, "assistant message content is empty")
    try:
        message = json.loads(content)
    except json.JSONDecodeError as error:
        raise AssertionError("assistant message content is not valid JSON") from error
    require(isinstance(message, dict), "assistant message content is not an object")
    require(
        message.get("role") == "assistant",
        "assistant message payload has the wrong role",
    )
    require("id" in message, "assistant message payload has no canonical id field")
    require(
        message["id"] is None or isinstance(message["id"], str),
        "assistant message payload has an invalid id",
    )
    blocks = message.get("content")
    require(isinstance(blocks, list), "assistant message payload has no content blocks")
    visible = [
        block["text"]
        for block in blocks
        if isinstance(block, dict)
        and set(block) == {"text"}
        and isinstance(block.get("text"), str)
    ]
    require(
        len(visible) == 1,
        "assistant message payload must have one canonical text block",
    )
    return "".join(visible)


def validate_assistant_rows(
    rows: list[dict[str, Any]],
    *,
    request_id: str,
    session_id: str,
    expected: str,
) -> None:
    assistant_rows = [row for row in rows if row.get("role") == "assistant"]
    require(len(assistant_rows) == 1, "expected exactly one durable assistant message")
    row = assistant_rows[0]
    require(row.get("request_id") == request_id, "assistant message request ID drifted")
    require(row.get("session_id") == session_id, "assistant message session ID drifted")
    output = visible_assistant_text(row.get("content"))
    require(output == expected, "durable assistant output differs from the challenge")


def validate_prompt_request(
    rows: list[dict[str, Any]],
    *,
    prompt: str,
    expected_session: str | None,
) -> tuple[str, str]:
    require(len(rows) == 1, "expected exactly one AgentRequest for the random prompt")
    row = rows[0]
    require(row.get("content") == prompt, "AgentRequest content does not match PTY prompt")
    require(row.get("behavior_id") == "port-live", "PTY request used the wrong behavior")
    require(row.get("lifecycle_state") == "completed", "PTY request is not completed")
    request_id = row.get("request_id")
    session_id = row.get("session_id")
    require(isinstance(request_id, str) and request_id, "PTY request ID is empty")
    require(isinstance(session_id, str) and session_id, "PTY session ID is empty")
    if expected_session is not None:
        require(session_id == expected_session, "PTY turns used different sessions")
    return request_id, session_id


def completed_request_for_prompt(
    graphql_url: str,
    prompt: str,
    *,
    expected: str,
    expected_session: str | None,
    deadline: float,
    query_rows: Callable[[str, str], list[dict[str, Any]]] = request_rows_for_prompt,
    query_messages: Callable[[str, str], list[dict[str, Any]]] = assistant_rows_for_request,
) -> tuple[str, str]:
    if time.monotonic() >= deadline:
        raise AssertionError("stock pager turn did not produce a completed AgentRequest")

    def attempt() -> tuple[str, str] | None:
        if time.monotonic() >= deadline:
            return None
        try:
            rows = query_rows(graphql_url, prompt)
            if len(rows) == 1 and rows[0].get("lifecycle_state") == "completed":
                request_id, session_id = validate_prompt_request(
                    rows,
                    prompt=prompt,
                    expected_session=expected_session,
                )
                messages = query_messages(graphql_url, request_id)
                if any(row.get("role") == "assistant" for row in messages):
                    validate_assistant_rows(
                        messages,
                        request_id=request_id,
                        session_id=session_id,
                        expected=expected,
                    )
                    return request_id, session_id
        except ProbeDeadlineExceeded:
            raise
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            if time.monotonic() >= deadline:
                raise AssertionError(
                    "live GraphQL remained unavailable while correlating the PTY request"
                ) from error
            return None
        require(len(rows) <= 1, "duplicate AgentRequests exist for the random PTY prompt")
        return None

    ready, result = poll_until_deadline(
        attempt,
        lambda candidate: candidate is not None,
        deadline=deadline,
        interval=0.2,
    )
    if ready:
        require(result is not None, "ready PTY correlation has no request")
        return result
    raise AssertionError("stock pager turn did not produce a completed AgentRequest")


def listener_pids(port: int) -> list[int]:
    result = subprocess.run(
        ["lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode not in {0, 1}:
        raise AssertionError(f"lsof failed while resolving listener: {result.stderr.strip()}")
    return sorted({int(line) for line in result.stdout.splitlines() if line.strip()})


def preflight_probe(args: argparse.Namespace) -> dict[str, Any]:
    port = graphql_port(args.graphql)
    port_vacant = listener_pids(port) == []
    socket_absent = not Path(args.socket).exists()
    require(port_vacant, "live GraphQL port is already occupied before launch")
    require(socket_absent, "leader socket already exists before launch")
    return {
        "version": 1,
        "nonce": secrets.token_hex(16),
        "created_unix_ms": int(time.time() * 1000),
        "graphql": args.graphql,
        "socket": str(Path(args.socket).resolve()),
        "preflight_port_vacant": True,
        "preflight_socket_absent": True,
    }


def load_preflight(path: str, *, graphql: str, socket_path: str) -> dict[str, Any]:
    value = json.loads(Path(path).read_text())
    require(isinstance(value, dict), "preflight JSON is not an object")
    require(value.get("version") == 1, "preflight version drifted")
    require(value.get("preflight_port_vacant") is True, "preflight port was not vacant")
    require(value.get("preflight_socket_absent") is True, "preflight socket was not absent")
    require(value.get("graphql") == graphql, "preflight GraphQL URL differs")
    require(
        value.get("socket") == str(Path(socket_path).resolve()),
        "preflight socket differs",
    )
    created = value.get("created_unix_ms")
    require(isinstance(created, int) and not isinstance(created, bool), "bad preflight time")
    age_ms = int(time.time() * 1000) - created
    require(0 <= age_ms <= 600_000, "preflight proof is stale")
    nonce = value.get("nonce")
    require(isinstance(nonce, str) and len(nonce) == 32, "bad preflight nonce")
    return value


def option_value(tokens: list[str], name: str) -> str | None:
    for index, token in enumerate(tokens):
        if token == name and index + 1 < len(tokens):
            return tokens[index + 1]
        prefix = f"{name}="
        if token.startswith(prefix):
            return token[len(prefix) :]
    return None


def process_command(
    pid: int,
    *,
    expected_home: str,
    expected_port: int,
    expected_socket: str,
) -> str:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    require(result.returncode == 0, f"listener PID {pid} is not running")
    command = result.stdout.strip()
    require(command, "listener command is empty")
    tokens = shlex.split(command)
    require(any(Path(token).name == "gents" for token in tokens), "listener binary is not gents")
    require("server" in tokens, "listener is not a gents server command")
    home = option_value(tokens, "--home")
    port = option_value(tokens, "--http-port")
    socket_path = option_value(tokens, "--grok-shim-socket-path")
    require(home is not None, "listener command has no --home")
    require(
        Path(home).resolve() == Path(expected_home).resolve(),
        "listener does not use the run-owned live home",
    )
    require(port == str(expected_port), "listener command has the wrong HTTP port")
    require(socket_path is not None, "listener command has no Grok socket path")
    require(
        Path(socket_path).resolve() == Path(expected_socket).resolve(),
        "listener command has the wrong Grok socket path",
    )
    return command


def read_available(fd: int, timeout: float) -> bytes:
    chunks: list[bytes] = []
    ready, _, _ = select.select([fd], [], [], timeout)
    while ready:
        try:
            chunk = os.read(fd, 65536)
        except OSError as error:
            if error.errno == errno.EIO:
                break
            raise
        if not chunk:
            break
        chunks.append(chunk)
        ready, _, _ = select.select([fd], [], [], 0)
    return b"".join(chunks)


def set_pty_size(fd: int, *, rows: int = 40, columns: int = 120) -> None:
    """Give full-screen clients a real viewport before they initialize."""
    require(rows > 0 and columns > 0, "PTY dimensions must be positive")
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, columns, 0, 0))


def bounded_deadline(overall_deadline: float, phase_timeout: float) -> float:
    require(phase_timeout > 0, "probe phase timeout must be positive")
    return min(overall_deadline, time.monotonic() + phase_timeout)


def read_until_quiet(
    fd: int,
    *,
    deadline: float,
    quiet_seconds: float,
    predicate: Callable[[bytes], bool] | None = None,
) -> tuple[bytes, bool]:
    data = bytearray()
    last_output: float | None = None
    matched = False
    while time.monotonic() < deadline:
        chunk = read_available(fd, min(0.2, max(0.0, deadline - time.monotonic())))
        if chunk:
            data.extend(chunk)
            last_output = time.monotonic()
            if predicate is None or predicate(bytes(data)):
                matched = True
        elif (
            matched
            and last_output is not None
            and time.monotonic() - last_output >= quiet_seconds
        ):
            return bytes(data), True
    return bytes(data), False


def terminate_client(process: subprocess.Popen[bytes], master_fd: int) -> None:
    if process.poll() is None:
        for _ in range(2):
            try:
                os.write(master_fd, b"\x03")
            except OSError as error:
                if error.errno not in {errno.EBADF, errno.EIO}:
                    raise
                break
            try:
                process.wait(timeout=2)
                break
            except subprocess.TimeoutExpired:
                pass
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                return
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                return


def challenge_prompt() -> tuple[str, str, bytes]:
    challenge = secrets.token_hex(12)
    expected = f"GENTS_STOCK_{challenge}"
    prompt = f"Reply with exactly: {expected}".encode()
    return challenge, expected, prompt


def run_probe(args: argparse.Namespace, client_guard: ClientGuard) -> dict[str, Any]:
    overall_deadline = time.monotonic() + args.total_timeout
    expected_endpoint = args.expected_endpoint
    preflight = load_preflight(args.preflight, graphql=args.graphql, socket_path=args.socket)
    applied_endpoint = queried_backend_endpoint(args.graphql, args.backend_id)
    validate_endpoint(
        expected_endpoint,
        applied_endpoint,
        os.environ.get("GENTS_GROK_PORT_ENDPOINT_1"),
        os.environ.get("GENTS_GROK_PORT_ENDPOINT"),
    )

    port = graphql_port(args.graphql)
    actual_pids = listener_pids(port)
    validate_listener_identity(args.listener_pid, actual_pids)
    command = process_command(
        args.listener_pid,
        expected_home=args.expected_home,
        expected_port=port,
        expected_socket=args.socket,
    )
    socket_path = Path(args.socket)
    require(socket_path.exists(), "leader socket does not exist")
    require(stat.S_ISSOCK(socket_path.stat().st_mode), "leader path is not a socket")
    challenge, expected, prompt_bytes = challenge_prompt()
    expected_bytes = expected.encode()
    idle_challenge, idle_expected, idle_prompt_bytes = challenge_prompt()

    with blocked_signal(signal.SIGALRM):
        master_fd, slave_fd = pty.openpty()
        client_guard.master_fd = master_fd
        client_guard.slave_fd = slave_fd
    process: subprocess.Popen[bytes] | None = None
    with blocked_signal(signal.SIGALRM):
        pager_socket = PortableSocketPath(args.socket)
        client_guard.pager_socket = pager_socket
    set_pty_size(slave_fd)
    env = os.environ.copy()
    env.setdefault("TERM", "xterm-256color")
    with blocked_signal(signal.SIGALRM):
        process = subprocess.Popen(
            [
                args.grok_bin,
                "--leader",
                "--leader-socket",
                pager_socket.connect_path,
                "--cwd",
                args.cwd,
                "--no-alt-screen",
                "--minimal",
            ],
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            cwd=args.cwd,
            env=env,
            start_new_session=True,
        )
        client_guard.process = process
    os.close(slave_fd)
    client_guard.slave_fd = None
    pre_submit, ready_quiet = read_until_quiet(
        master_fd,
        deadline=bounded_deadline(overall_deadline, args.ready_timeout),
        quiet_seconds=args.quiet_seconds,
    )
    require(ready_quiet and process.poll() is None, "stock pager did not become input-ready")
    require(pre_submit, "stock pager produced no pre-submit UI")
    require(expected_bytes not in pre_submit, "expected marker appeared before Enter")
    os.write(master_fd, prompt_bytes)
    time.sleep(0.15)
    os.write(master_fd, b"\r")
    first_request_id, stock_session_id = (
        completed_request_for_prompt(
            args.graphql,
            prompt_bytes.decode(),
            expected=expected,
            expected_session=None,
            deadline=bounded_deadline(overall_deadline, args.timeout),
        )
    )
    _post_submit, first_quiet = read_until_quiet(
        master_fd,
        deadline=bounded_deadline(overall_deadline, args.timeout),
        quiet_seconds=args.quiet_seconds,
    )
    require(first_quiet and process.poll() is None, "first stock pager response did not settle")
    read_available(master_fd, 0.5)
    os.write(master_fd, idle_prompt_bytes)
    time.sleep(0.15)
    os.write(master_fd, b"\r")
    second_request_id, second_session_id = (
        completed_request_for_prompt(
            args.graphql,
            idle_prompt_bytes.decode(),
            expected=idle_expected,
            expected_session=stock_session_id,
            deadline=bounded_deadline(overall_deadline, args.timeout),
        )
    )
    _idle_output, second_quiet = read_until_quiet(
        master_fd,
        deadline=bounded_deadline(overall_deadline, args.timeout),
        quiet_seconds=args.quiet_seconds,
    )
    require(
        second_quiet and process.poll() is None,
        "input-ready proof response did not settle",
    )
    require(second_request_id != first_request_id, "input-ready proof reused the first request")
    require(second_session_id == stock_session_id, "input-ready proof changed sessions")
    idle_transition = process.poll() is None
    require(idle_transition, "stock pager exited before completing the second turn")

    proof: dict[str, Any] = {
        "version": 1,
        "preflight_nonce": preflight["nonce"],
        "preflight_port_vacant": True,
        "preflight_socket_absent": True,
        "expected_endpoint": expected_endpoint,
        "applied_endpoint": applied_endpoint,
        "backend_id": args.backend_id,
        "live_home": args.expected_home,
        "live_graphql": args.graphql,
        "live_socket": args.socket,
        "endpoint_env_name": "GENTS_GROK_PORT_ENDPOINT_1",
        "endpoint_verified": True,
        "listener_pid": args.listener_pid,
        "listener_command": command,
        "listener_verified": True,
        "pty_challenge": challenge,
        "pty_expected": expected,
        "pty_prompt_sha256": hashlib.sha256(prompt_bytes).hexdigest(),
        "pty_pre_submit_bytes": len(pre_submit),
        "pty_session_id": stock_session_id,
        "pty_terminal_request_id": first_request_id,
        "pty_idle_challenge": idle_challenge,
        "pty_idle_expected": idle_expected,
        "pty_idle_probe_request_id": second_request_id,
        "pty_idle_transition": True,
        "pty_verified": True,
        "cleanup_listener_absent": False,
        "cleanup_socket_absent": False,
    }
    return proof


def process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def validate_cleanup_targets(
    proof: dict[str, Any],
    *,
    graphql: str,
    socket_path: str,
) -> None:
    require(proof.get("live_graphql") == graphql, "cleanup GraphQL URL differs from proof")
    proved_socket = proof.get("live_socket")
    require(isinstance(proved_socket, str) and proved_socket, "proof live socket is empty")
    require(
        Path(proved_socket).resolve() == Path(socket_path).resolve(),
        "cleanup socket differs from proof",
    )


def cleanup_probe(args: argparse.Namespace) -> dict[str, Any]:
    proof = json.loads(Path(args.proof).read_text())
    require(isinstance(proof, dict), "proof JSON is not an object")
    validate_cleanup_targets(proof, graphql=args.graphql, socket_path=args.socket)
    listener_pid = int(proof["listener_pid"])
    port = graphql_port(proof["live_graphql"])
    listener_absent = not process_exists(listener_pid) and listener_pids(port) == []
    socket_absent = not Path(proof["live_socket"]).exists()
    require(listener_absent, "recorded listener or live GraphQL port remains active")
    require(socket_absent, "leader socket remains after cleanup")
    proof["cleanup_listener_absent"] = True
    proof["cleanup_socket_absent"] = True
    return proof


def self_test() -> dict[str, int]:
    accepted = 0
    rejected = 0

    def accept(call: Callable[[], None]) -> None:
        nonlocal accepted
        call()
        accepted += 1

    def reject(call: Callable[[], None]) -> None:
        nonlocal rejected
        try:
            call()
        except AssertionError:
            rejected += 1
            return
        raise AssertionError("negative live-gate fixture unexpectedly passed")

    self_test_portable_socket_path()

    read_fd, write_fd = os.pipe()

    def delayed_ready_output() -> None:
        try:
            time.sleep(0.03)
            os.write(write_fd, b"ready")
        finally:
            os.close(write_fd)

    ready_started = time.monotonic()
    ready_writer = threading.Thread(target=delayed_ready_output)
    ready_writer.start()
    try:
        ready_data, ready_quiet = read_until_quiet(
            read_fd,
            deadline=ready_started + 2.0,
            quiet_seconds=0.01,
        )
    finally:
        ready_writer.join()
        os.close(read_fd)
    require(ready_data == b"ready" and ready_quiet, "delayed ready output was lost")
    require(
        time.monotonic() - ready_started >= 0.03,
        "ready quiet timer armed before the first byte",
    )

    pty_master, pty_slave = pty.openpty()
    try:
        set_pty_size(pty_slave, rows=37, columns=109)
        size = struct.unpack(
            "HHHH", fcntl.ioctl(pty_slave, termios.TIOCGWINSZ, b"\0" * 8)
        )
        require(size[:2] == (37, 109), "PTY viewport size was not applied")
    finally:
        os.close(pty_master)
        os.close(pty_slave)

    try:
        with hard_deadline(0.03):
            time.sleep(0.2)
    except ProbeDeadlineExceeded:
        pass
    else:
        raise AssertionError("hard probe deadline did not fire")

    class FakeProcess:
        pid = 4242

    guarded = ClientGuard()
    guarded.process = FakeProcess()  # type: ignore[assignment]
    killed: list[tuple[int, signal.Signals]] = []
    cleaned: list[str] = []
    prior_alarm_handler = signal.getsignal(signal.SIGALRM)

    def record_deadline_cleanup() -> None:
        require(
            signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0),
            "deadline remained armed during cleanup",
        )
        require(
            signal.getsignal(signal.SIGALRM) == prior_alarm_handler,
            "SIGALRM handler was not restored before cleanup",
        )
        cleaned.append("deadline")

    guarded.cleanup = record_deadline_cleanup  # type: ignore[method-assign]
    original_killpg = os.killpg
    os.killpg = lambda pid, sig: killed.append((pid, sig))  # type: ignore[assignment]
    try:
        try:
            def pending_deadline() -> dict[str, Any]:
                with blocked_signal(signal.SIGALRM):
                    time.sleep(0.06)

                return {}

            run_with_deadline_cleanup(0.03, guarded, pending_deadline)
        except ProbeDeadlineExceeded:
            pass
        else:
            raise AssertionError("pending deadline did not fire after signal unblock")
    finally:
        os.killpg = original_killpg
    require(
        killed == [(4242, signal.SIGKILL)],
        "deadline did not kill the guarded pager before raising",
    )
    require(cleaned == ["deadline"], "deadline cleanup did not run exactly once")

    def ordinary_failure() -> dict[str, Any]:
        raise AssertionError("ordinary failure")

    for label, work in (("success", lambda: {}), ("error", ordinary_failure)):
        cleanup_guard = ClientGuard()
        cleanup_calls: list[str] = []
        cleanup_guard.cleanup = lambda label=label: cleanup_calls.append(label)  # type: ignore[method-assign]
        try:
            run_with_deadline_cleanup(1.0, cleanup_guard, work)
        except AssertionError:
            require(label == "error", "successful deadline wrapper raised")
        require(cleanup_calls == [label], f"{label} cleanup did not run exactly once")

    class FaultProcess:
        pid = 4343
        alive = True

        def poll(self) -> int | None:
            return None if self.alive else -signal.SIGKILL

        def wait(self, timeout: float) -> int:
            del timeout
            fault_events.append("wait")
            return -signal.SIGKILL

    class FaultSocket:
        def cleanup(self) -> None:
            fault_events.append("socket")
            raise OSError(errno.ENOSPC, "injected socket cleanup failure")

    fault_events: list[str] = []
    fault_process = FaultProcess()
    fault_guard = ClientGuard()
    fault_guard.process = fault_process  # type: ignore[assignment]
    fault_guard.master_fd = 12
    fault_guard.slave_fd = 11
    fault_guard.pager_socket = FaultSocket()  # type: ignore[assignment]
    original_close = os.close
    original_killpg = os.killpg
    original_terminate_client = globals()["terminate_client"]
    slave_error = OSError(errno.EIO, "injected slave close failure")

    def fault_close(fd: int) -> None:
        fault_events.append(f"close:{fd}")
        if fd == 11:
            raise slave_error
        raise OSError(errno.EBADF, "injected master close failure")

    def fault_terminate(_process: Any, _master_fd: int) -> None:
        fault_events.append("terminate")
        raise OSError(errno.EIO, "injected terminate failure")

    def fault_killpg(pid: int, sig: signal.Signals) -> None:
        require((pid, sig) == (4343, signal.SIGKILL), "fallback kill drifted")
        fault_events.append("kill")
        fault_process.alive = False

    os.close = fault_close  # type: ignore[assignment]
    os.killpg = fault_killpg  # type: ignore[assignment]
    globals()["terminate_client"] = fault_terminate
    try:
        try:
            fault_guard.cleanup()
        except OSError as error:
            require(error is slave_error, "cleanup did not preserve its first error")
        else:
            raise AssertionError("injected cleanup failure was swallowed")
    finally:
        os.close = original_close  # type: ignore[assignment]
        os.killpg = original_killpg  # type: ignore[assignment]
        globals()["terminate_client"] = original_terminate_client
    require(
        fault_events
        == ["close:11", "terminate", "kill", "wait", "socket", "close:12"],
        f"cleanup did not release every owned resource: {fault_events}",
    )

    class FullyFaultingProcess:
        pid = 4444

        def poll(self) -> int | None:
            full_fault_events.append("poll")
            raise poll_error

        def wait(self, timeout: float) -> int:
            full_fault_events.append("wait")
            raise subprocess.TimeoutExpired("pager", timeout)

    class RecordingSocket:
        def cleanup(self) -> None:
            full_fault_events.append("socket")

    full_fault_events: list[str] = []
    poll_error = RuntimeError("injected poll failure")
    full_fault_guard = ClientGuard()
    full_fault_guard.process = FullyFaultingProcess()  # type: ignore[assignment]
    full_fault_guard.master_fd = 14
    full_fault_guard.pager_socket = RecordingSocket()  # type: ignore[assignment]
    original_close = os.close
    original_killpg = os.killpg
    original_terminate_client = globals()["terminate_client"]

    def full_fault_close(fd: int) -> None:
        full_fault_events.append(f"close:{fd}")

    def full_fault_terminate(_process: Any, _master_fd: int) -> None:
        full_fault_events.append("terminate")

    def full_fault_kill(_pid: int, _sig: signal.Signals) -> None:
        full_fault_events.append("kill")
        raise PermissionError("injected kill failure")

    os.close = full_fault_close  # type: ignore[assignment]
    os.killpg = full_fault_kill  # type: ignore[assignment]
    globals()["terminate_client"] = full_fault_terminate
    try:
        try:
            full_fault_guard.cleanup()
        except Exception as error:
            require(error is poll_error, "poll failure was not preserved as first error")
        else:
            raise AssertionError("fully faulting cleanup unexpectedly succeeded")
    finally:
        os.close = original_close  # type: ignore[assignment]
        os.killpg = original_killpg  # type: ignore[assignment]
        globals()["terminate_client"] = original_terminate_client
    require(
        full_fault_events == ["terminate", "poll", "kill", "wait", "socket", "close:14"],
        f"cleanup stopped after a secondary failure: {full_fault_events}",
    )

    transient_calls = 0

    def deadline_during_query(_graphql: str, _prompt: str) -> list[dict[str, Any]]:
        raise ProbeDeadlineExceeded("injected hard deadline")

    try:
        completed_request_for_prompt(
            "http://unused.invalid/graphql",
            "deadline-prompt",
            expected="unused",
            expected_session=None,
            deadline=time.monotonic() + 1.0,
            query_rows=deadline_during_query,
        )
    except ProbeDeadlineExceeded:
        pass
    else:
        raise AssertionError("hard deadline was swallowed by transient polling")

    def transient_then_complete(_graphql: str, prompt: str) -> list[dict[str, Any]]:
        nonlocal transient_calls
        transient_calls += 1
        if transient_calls == 1:
            raise urllib.error.URLError("transient self-test failure")
        return [
            {
                "request_id": "request-1",
                "session_id": "session-1",
                "behavior_id": "port-live",
                "content": prompt,
                "lifecycle_state": "completed",
            }
        ]

    message_calls = 0

    def transient_messages(_graphql: str, _request_id: str) -> list[dict[str, Any]]:
        nonlocal message_calls
        message_calls += 1
        if message_calls == 1:
            raise urllib.error.URLError("transient message hydration failure")
        return [
            {
                "request_id": "request-1",
                "session_id": "session-1",
                "sequence": 2,
                "role": "assistant",
                "content": json.dumps(
                    {
                        "role": "assistant",
                        "id": None,
                        "content": [{"text": "answer-1"}],
                    }
                ),
            }
        ]

    transient_result = completed_request_for_prompt(
        "http://unused.invalid/graphql",
        "prompt-1",
        expected="answer-1",
        expected_session="session-1",
        deadline=time.monotonic() + 1.0,
        query_rows=transient_then_complete,
        query_messages=transient_messages,
    )
    require(
        transient_calls == 3
        and message_calls == 2
        and transient_result == ("request-1", "session-1"),
        "transient GraphQL polling did not recover deterministically",
    )

    expired_calls = 0

    def remain_incomplete(_graphql: str, _prompt: str) -> list[dict[str, Any]]:
        nonlocal expired_calls
        expired_calls += 1
        return []

    clock_values = iter((1.0, 1.0, 1.0, 3.0, 3.0))
    original_monotonic = time.monotonic
    original_sleep = time.sleep
    time.monotonic = lambda: next(clock_values)  # type: ignore[assignment]
    time.sleep = lambda _seconds: None  # type: ignore[assignment]
    try:
        try:
            completed_request_for_prompt(
                "http://unused.invalid/graphql",
                "incomplete-prompt",
                expected="unused",
                expected_session=None,
                deadline=2.0,
                query_rows=remain_incomplete,
            )
        except AssertionError as error:
            require(
                str(error) == "stock pager turn did not produce a completed AgentRequest",
                "stock incomplete timeout changed shape",
            )
        else:
            raise AssertionError("stock incomplete timeout unexpectedly succeeded")
    finally:
        time.monotonic = original_monotonic  # type: ignore[assignment]
        time.sleep = original_sleep  # type: ignore[assignment]
    require(expired_calls == 1, "stock polling queried again after its deadline")

    accept(lambda: validate_endpoint("http://one", "http://one", "http://one", None))
    reject(lambda: validate_endpoint("http://one", "http://two", "http://one", None))
    reject(lambda: validate_endpoint("http://one", "http://one", "http://two", None))
    reject(lambda: validate_endpoint("http://one", "http://one", "http://one", "http://old"))
    queried = backend_endpoint_from_data(
        {"InferenceBackend": [{"backend_id": "backend", "endpoint": "http://wrong"}]},
        "backend",
    )
    reject(lambda: validate_endpoint("http://right", queried, "http://right", None))
    accept(lambda: validate_listener_identity(42, [42]))
    reject(lambda: validate_listener_identity(41, [42]))

    good_row = {
        "request_id": "request-1",
        "session_id": "session-1",
        "behavior_id": "port-live",
        "content": "prompt-1",
        "lifecycle_state": "completed",
    }
    accept(
        lambda: validate_prompt_request(
            [good_row], prompt="prompt-1", expected_session="session-1"
        )
    )
    reject(
        lambda: validate_prompt_request(
            [{**good_row, "content": "unrelated"}],
            prompt="prompt-1",
            expected_session="session-1",
        )
    )
    reject(
        lambda: validate_prompt_request(
            [{**good_row, "session_id": "session-2"}],
            prompt="prompt-1",
            expected_session="session-1",
        )
    )
    reject(
        lambda: validate_prompt_request(
            [{**good_row, "behavior_id": "other"}],
            prompt="prompt-1",
            expected_session="session-1",
        )
    )
    reject(
        lambda: validate_prompt_request(
            [{**good_row, "lifecycle_state": "processing"}],
            prompt="prompt-1",
            expected_session="session-1",
        )
    )

    cleanup_proof = {
        "live_graphql": "http://127.0.0.1:19000/api/v0/graphql",
        "live_socket": "/tmp/gents-live-proof.sock",
    }
    accept(
        lambda: validate_cleanup_targets(
            cleanup_proof,
            graphql="http://127.0.0.1:19000/api/v0/graphql",
            socket_path="/tmp/gents-live-proof.sock",
        )
    )
    reject(
        lambda: validate_cleanup_targets(
            cleanup_proof,
            graphql="http://127.0.0.1:19001/api/v0/graphql",
            socket_path="/tmp/gents-live-proof.sock",
        )
    )
    reject(
        lambda: validate_cleanup_targets(
            cleanup_proof,
            graphql="http://127.0.0.1:19000/api/v0/graphql",
            socket_path="/tmp/unrelated.sock",
        )
    )

    good_assistant = {
        "request_id": "request-1",
        "session_id": "session-1",
        "sequence": 2,
        "role": "assistant",
        "content": json.dumps(
            {"role": "assistant", "id": None, "content": [{"text": "answer-1"}]}
        ),
    }

    def assistant_fixture(message: dict[str, Any]) -> list[dict[str, Any]]:
        return [{**good_assistant, "content": json.dumps(message)}]

    canonical_assistant = {
        "role": "assistant",
        "id": None,
        "content": [{"text": "answer-1"}],
    }
    accept(
        lambda: validate_assistant_rows(
            [good_assistant],
            request_id="request-1",
            session_id="session-1",
            expected="answer-1",
        )
    )
    reject(
        lambda: validate_assistant_rows(
            assistant_fixture(
                {
                    key: value
                    for key, value in canonical_assistant.items()
                    if key != "id"
                }
            ),
            request_id="request-1",
            session_id="session-1",
            expected="answer-1",
        )
    )
    reject(
        lambda: validate_assistant_rows(
            assistant_fixture({**canonical_assistant, "role": "user"}),
            request_id="request-1",
            session_id="session-1",
            expected="answer-1",
        )
    )
    reject(
        lambda: validate_assistant_rows(
            assistant_fixture({**canonical_assistant, "content": {}}),
            request_id="request-1",
            session_id="session-1",
            expected="answer-1",
        )
    )
    reject(
        lambda: validate_assistant_rows(
            assistant_fixture(
                {
                    **canonical_assistant,
                    "content": [{"text": "answer-1", "extra": True}],
                }
            ),
            request_id="request-1",
            session_id="session-1",
            expected="answer-1",
        )
    )
    return {"accepted": accepted, "rejected": rejected}


def write_json(value: dict[str, Any], output: str | None) -> None:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"))
    if output:
        require(len(encoded.encode()) <= 4_000, "proof JSON exceeds two datastore strings")
        Path(output).write_text(encoded)
    sys.stdout.write(json.dumps(value, sort_keys=True, indent=2) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("self-test")

    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--socket", required=True)
    preflight.add_argument("--graphql", required=True)
    preflight.add_argument("--output")

    run = subparsers.add_parser("run")
    run.add_argument("--socket", required=True)
    run.add_argument("--graphql", required=True)
    run.add_argument("--expected-endpoint", required=True)
    run.add_argument("--backend-id", required=True)
    run.add_argument("--expected-home", required=True)
    run.add_argument("--preflight", required=True)
    run.add_argument("--listener-pid", required=True, type=int)
    run.add_argument("--grok-bin", default="grok")
    run.add_argument("--cwd", default=os.getcwd())
    run.add_argument("--ready-timeout", type=float, default=15.0)
    run.add_argument("--timeout", type=float, default=90.0)
    run.add_argument(
        "--total-timeout",
        type=float,
        default=95.0,
        help="overall run deadline; defaults below the runtime's 120s shell ceiling",
    )
    run.add_argument("--quiet-seconds", type=float, default=2.0)
    run.add_argument("--output")

    cleanup = subparsers.add_parser("cleanup")
    cleanup.add_argument("--proof", required=True)
    cleanup.add_argument("--socket", required=True)
    cleanup.add_argument("--graphql", required=True)
    cleanup.add_argument("--output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "self-test":
            write_json({"self_test": self_test()}, None)
        elif args.command == "preflight":
            write_json(preflight_probe(args), args.output)
        elif args.command == "run":
            client_guard = ClientGuard()
            proof = run_with_deadline_cleanup(
                args.total_timeout,
                client_guard,
                lambda: run_probe(args, client_guard),
            )
            write_json(proof, args.output)
        else:
            write_json(cleanup_probe(args), args.output)
    except (
        AssertionError,
        OSError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
        subprocess.TimeoutExpired,
    ) as error:
        print(json.dumps({"ok": False, "error": str(error)}, sort_keys=True), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
