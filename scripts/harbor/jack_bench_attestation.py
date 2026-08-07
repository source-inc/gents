"""Independent Jack Bench runtime receipt emitted by the pinned Harbor adapter."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import struct
from pathlib import Path
from typing import Any

EXPORT_SCHEMA = "jack-bench/harbor-export/v1"
RUNTIME_ATTESTATION_SCHEMA = "jack-bench-harbor-runtime-attestation/v1"
RUNTIME_ATTESTATION_FILE = "jack-bench-runtime-attestation.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(64 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def serde_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _hash_package_field(digest: Any, label: bytes, value: bytes) -> None:
    digest.update(struct.pack("<Q", len(label)))
    digest.update(label)
    digest.update(struct.pack("<Q", len(value)))
    digest.update(value)


def package_sha256(root: Path) -> str:
    """Reproduce Jack Bench's package payload hash, excluding its export record."""
    digest = hashlib.sha256()

    def visit(directory: Path) -> None:
        for path in sorted(directory.iterdir(), key=lambda candidate: candidate.name):
            relative = path.relative_to(root).as_posix()
            if relative == "jack-bench-export.json":
                continue
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                raise RuntimeError(f"Jack Bench package contains a symlink: {relative}")
            if stat.S_ISDIR(metadata.st_mode):
                _hash_package_field(digest, b"type", b"directory")
                _hash_package_field(digest, b"path", relative.encode())
                visit(path)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise RuntimeError(
                    f"Jack Bench package contains an unsupported entry: {relative}"
                )
            _hash_package_field(digest, b"type", b"file")
            _hash_package_field(digest, b"path", relative.encode())
            mode = 0o755 if metadata.st_mode & 0o111 else 0o644
            _hash_package_field(digest, b"mode", struct.pack("<I", mode))
            _hash_package_field(digest, b"size", struct.pack("<Q", metadata.st_size))
            digest.update(b"content")
            with path.open("rb") as source:
                while chunk := source.read(64 * 1024):
                    digest.update(chunk)

    visit(root)
    return digest.hexdigest()


def task_content_sha256(root: Path) -> str:
    """Reproduce Harbor 0.20.0's task-content identity for Jack packages."""
    files: set[str] = set()
    for relative in ("task.toml", "instruction.md", "README.md"):
        path = root / relative
        if path.exists():
            if path.is_symlink() or not path.is_file():
                raise RuntimeError(
                    f"Jack Bench task content entry is not a file: {relative}"
                )
            files.add(relative)
    for relative in ("environment", "tests", "solution", "steps"):
        directory = root / relative
        if not directory.exists():
            continue
        if directory.is_symlink() or not directory.is_dir():
            raise RuntimeError(
                f"Jack Bench task content entry is not a directory: {relative}"
            )
        for path in directory.rglob("*"):
            child = path.relative_to(root).as_posix()
            if path.is_symlink():
                raise RuntimeError(
                    f"Jack Bench task content contains a symlink: {child}"
                )
            if path.is_dir():
                continue
            if not path.is_file():
                raise RuntimeError(
                    f"Jack Bench task content contains an unsupported entry: {child}"
                )
            if "__pycache__" in path.parts:
                continue
            name = path.name
            if name == ".DS_Store" or name.endswith((".pyc", ".swp", ".swo", "~")):
                continue
            files.add(child)
    digest = hashlib.sha256()
    for relative in sorted(files):
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(sha256_file(root / relative).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def write_runtime_attestation(
    *,
    binary_path: str | None,
    controller_binary_sha256: str | None,
    controller_entrypoint: Path,
    detected_gents_version: str,
    harbor_version: str,
    logs_dir: Path,
) -> None:
    """Verify observed runtime bytes and exclusively retain a Jack receipt."""
    if not binary_path:
        raise RuntimeError("Jack Bench runtime attestation requires GENTS_BINARY_PATH")
    binary = Path(binary_path).resolve(strict=True)
    package = binary.parent.parent
    export_path = package / "jack-bench-export.json"
    try:
        export_artifact = json.loads(export_path.read_text())
    except (OSError, ValueError) as error:
        raise RuntimeError(
            f"Jack Bench export record is unavailable: {export_path}"
        ) from error
    if not isinstance(export_artifact, dict):
        raise RuntimeError("Jack Bench export record is not an object")
    if export_artifact.get("artifact_schema") != EXPORT_SCHEMA:
        raise RuntimeError("Jack Bench export record uses an unsupported schema")
    export = export_artifact.get("payload")
    if not isinstance(export, dict):
        raise RuntimeError("Jack Bench export record has no object payload")
    if export_artifact.get("payload_sha256") != serde_json_sha256(export):
        raise RuntimeError("Jack Bench export record payload hash mismatch")

    expected_package = export.get("package_payload_sha256")
    if not isinstance(expected_package, str):
        raise RuntimeError("Jack Bench export omits its package payload hash")
    if package_sha256(package) != expected_package:
        raise RuntimeError("Jack Bench package payload hash mismatch at runtime")
    observed_task_content = task_content_sha256(package)

    adapter_files = export.get("gents_adapter_files_sha256")
    if not isinstance(adapter_files, dict) or not adapter_files:
        raise RuntimeError("Jack Bench export omits adapter source identities")
    observed_adapter_files: dict[str, str] = {}
    for relative, expected in adapter_files.items():
        if not isinstance(relative, str) or not isinstance(expected, str):
            raise RuntimeError("Jack Bench adapter source identity is invalid")
        source = package / relative
        try:
            resolved_source = source.resolve(strict=True)
            resolved_source.relative_to(package)
        except (OSError, ValueError) as error:
            raise RuntimeError(
                f"Jack Bench adapter source escapes the package: {relative}"
            ) from error
        if source.is_symlink() or not resolved_source.is_file():
            raise RuntimeError(f"Jack Bench adapter source is invalid: {relative}")
        observed = sha256_file(resolved_source)
        if observed != expected:
            raise RuntimeError(f"Jack Bench adapter source hash mismatch: {relative}")
        observed_adapter_files[relative] = observed

    gents = export.get("gents") or {}
    harbor = export.get("harbor") or {}
    if not isinstance(gents, dict) or not isinstance(harbor, dict):
        raise RuntimeError("Jack Bench export omits runtime identities")
    if binary.relative_to(package).as_posix() != gents.get("package_path"):
        raise RuntimeError("Jack Bench Gents binary path does not match the export")
    observed_binary = sha256_file(binary)
    if observed_binary != gents.get("sha256"):
        raise RuntimeError("Jack Bench Gents binary hash mismatch at runtime")
    agent_version = gents.get("commit")
    if not isinstance(agent_version, str) or agent_version not in detected_gents_version:
        raise RuntimeError(
            "Installed Gents version does not identify the exported source commit"
        )

    if not controller_binary_sha256 or not re.fullmatch(
        r"[0-9a-f]{64}", controller_binary_sha256
    ):
        raise RuntimeError(
            "Jack Bench runtime attestation requires a controller binary hash"
        )
    controller = controller_entrypoint.resolve(strict=True)
    observed_controller = sha256_file(controller)
    if observed_controller != controller_binary_sha256:
        raise RuntimeError("Harbor controller binary hash mismatch at runtime")
    if harbor_version != harbor.get("version"):
        raise RuntimeError("Harbor runtime version does not match the export")

    attestation = {
        "schema_version": RUNTIME_ATTESTATION_SCHEMA,
        "package_payload_sha256": expected_package,
        "task_content_sha256": observed_task_content,
        "harbor_version": harbor_version,
        "harbor_commit": harbor.get("commit"),
        "controller_binary_sha256": observed_controller,
        "agent_adapter": harbor.get("agent_adapter"),
        "agent_version": agent_version,
        "agent_source_sha256": serde_json_sha256(observed_adapter_files),
        "agent_binary_sha256": observed_binary,
        "environment_image": export.get("environment_image"),
        "verifier_image": export.get("verifier_image"),
        "platform": export.get("platform"),
    }
    output = logs_dir.parent / RUNTIME_ATTESTATION_FILE
    encoded = json.dumps(attestation, indent=2, sort_keys=True).encode()
    try:
        with output.open("xb") as destination:
            destination.write(encoded)
            destination.flush()
            os.fsync(destination.fileno())
        directory = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except FileExistsError as error:
        raise RuntimeError(
            f"Jack Bench runtime attestation already exists: {output}"
        ) from error
