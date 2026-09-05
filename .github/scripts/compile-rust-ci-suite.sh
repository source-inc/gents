#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <runtime|cli|desktop|support> [--list-packages]" >&2
  exit 2
fi

suite="$1"
mode="${2:-build}"
packages=()
separate_package=""

case "${suite}" in
  runtime)
    packages=(gents)
    ;;
  cli)
    packages=(
      gents-cli
    )
    ;;
  desktop)
    packages=(
      gents-desktop-core
      gents-desktop
      gents-desktop-bridge
      fixture-domain-plugin
      gents-fixture-host
    )
    # Keep Tauri in its own Cargo invocation; its target surface and feature
    # graph are substantially heavier than the reusable desktop crates.
    separate_package="gents-desktop-tauri"
    ;;
  support)
    # gents-chatgpt-login, gents-claude-login and gents-codex-protocol live here (not in `cli`)
    # because the support shard is where their tests RUN on every event; the
    # compile fence must cover what the test step executes. They are still
    # built on the cli shard as ordinary gents-cli dependencies.
    packages=(
      gents-lean-contract
      gents-migration
      gents-lens-fixture-add-label
      gents-callback-fixture-create-workspace
      gents-fs-runner
      gents-protocol
      gents-schemas
      gents-chatgpt-login
      gents-claude-login
      gents-codex-protocol
    )
    ;;
  *)
    echo "unknown Rust CI suite: ${suite}" >&2
    exit 2
    ;;
esac

if [[ "${mode}" == "--list-packages" ]]; then
  printf '%s\n' "${packages[@]}"
  if [[ -n "${separate_package}" ]]; then
    printf '%s\n' "${separate_package}"
  fi
  exit 0
fi
if [[ "${mode}" != "build" ]]; then
  echo "unknown mode: ${mode}" >&2
  exit 2
fi

# The support shard deliberately reuses a target directory across checkouts.
# Cargo's mtime fast path can otherwise accept a stale local crate when a
# checkout adds a new module, as happened when gents-protocol became the
# ChatGPT OAuth vocabulary owner. Preserve dependency artifacts, but always
# rebuild workspace members in this shard with its direct rustc wrapper.
if [[ "${suite}" == "support" ]]; then
  cargo clean --workspace
fi

cargo_args=()
for package in "${packages[@]}"; do
  cargo_args+=(-p "${package}")
done
cargo test "${cargo_args[@]}" --all-targets --no-run

if [[ -n "${separate_package}" ]]; then
  cargo test -p "${separate_package}" --all-targets --no-run
fi
