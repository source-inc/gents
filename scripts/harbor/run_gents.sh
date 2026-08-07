#!/bin/sh
set -eu

# Classify a terminal response document. Budget exhaustion is the only
# terminal error Harbor should verifier-score: the workspace may hold real
# work. The match is anchored to the full owned-loop error shape — the
# max-turn guard's `PromptError::MaxTurnsError` display as persisted by
# `agent stream failed: {error}` (pinned by the runtime max-turns test) at
# the very start of the error_message value. A provider error that merely
# echoes upstream text mentioning MaxTurnError therefore stays an agent
# exception, as does everything else unrecognized. Matching the quoted
# key-plus-prefix is safe against model content: JSON escapes quotes inside
# string values, so this byte sequence can only introduce the real field.
_MAX_TURN_ERROR_PREFIX='"error_message": "agent stream failed: PromptError: MaxTurnError: '
_AGGREGATE_TOKEN_ERROR_PREFIX='"error_message": "agent stream failed: CompletionError: ProviderError: aggregate_token_budget_exhausted: '
_COMPACTION_PROVIDER_ERROR='compaction_provider_failure:'

response_error_has() {
  response_file=$1
  expected=$2
  sed -n '/^[[:space:]]*"error_message":/p' "${response_file}" |
    head -1 |
    grep -qF "${expected}"
}

# Read one non-negative integer from the root final_metrics.extra object in
# Gents' pretty-printed ATIF. Exact indentation anchors this to the top-level
# metrics object, so model-authored tool arguments cannot forge the settle gate.
atif_final_metrics_extra_u64() {
  atif_path=$1
  atif_key=$2
  awk -v key="${atif_key}" '
    /^  "final_metrics": \{$/ { in_final_metrics = 1; next }
    in_final_metrics && /^    "extra": \{$/ { in_extra = 1; next }
    in_extra && /^    \}/ { exit }
    in_extra && $1 == "\"" key "\":" {
      value = $2
      sub(/,$/, "", value)
      if (value ~ /^[0-9]+$/) print value
      exit
    }
  ' "${atif_path}"
}

normalize_token_budget_outcome() {
  candidate_outcome=$1
  usage_count=$2
  if [ "${candidate_outcome}" = "token_budget_exhausted" ] && \
    [ "${usage_count}" -eq 0 ]; then
    printf 'agent_error\n'
  else
    printf '%s\n' "${candidate_outcome}"
  fi
}

classify_response() {
  response_file=$1
  response_file_status=$(sed -n 's/^[[:space:]]*"status": "\([^"]*\)",*$/\1/p' "${response_file}" | head -1)
  case "${response_file_status}" in
    complete|completed)
      printf 'completed\n'
      ;;
    error)
      if grep -qF "${_MAX_TURN_ERROR_PREFIX}" "${response_file}"; then
        printf 'max_turns_exhausted\n'
      elif grep -qF "${_AGGREGATE_TOKEN_ERROR_PREFIX}" "${response_file}"; then
        printf 'token_budget_exhausted\n'
      elif response_error_has "${response_file}" "${_COMPACTION_PROVIDER_ERROR}"; then
        printf 'compaction_provider_error\n'
      else
        printf 'agent_error\n'
      fi
      ;;
    *)
      printf 'unexpected:%s\n' "${response_file_status:-missing}"
      ;;
  esac
}

# Fixture-driven check of terminal-response classification. Runs without any
# Gents environment; CI executes it next to the shell-syntax check.
run_self_test() {
  self_test_dir=$(mktemp -d /tmp/gents-harbor-self-test.XXXXXX)
  trap 'rm -rf "${self_test_dir}"' EXIT
  failures=0

  expect_outcome() {
    fixture_name=$1
    expected=$2
    fixture_file="${self_test_dir}/${fixture_name}.json"
    actual=$(classify_response "${fixture_file}")
    if [ "${actual}" = "${expected}" ]; then
      printf 'ok: %s -> %s\n' "${fixture_name}" "${actual}"
    else
      printf 'FAIL: %s expected %s, got %s\n' \
        "${fixture_name}" "${expected}" "${actual}" >&2
      failures=$((failures + 1))
    fi
  }

  cat >"${self_test_dir}/complete.json" <<'EOF'
{
  "request_id": "req-1",
  "status": "complete",
  "content": "done",
  "error_message": null
}
EOF
  cat >"${self_test_dir}/trajectory.json" <<'EOF'
{
  "steps": [
    {
      "observation": {
        "final_metrics": {
          "extra": {
            "inference_call_count": 999
          }
        }
      }
    }
  ],
  "final_metrics": {
    "total_steps": 1,
    "extra": {
      "inference_call_count": 2,
      "inference_call_pending_count": 0,
      "inference_call_usage_count": 2
    }
  }
}
EOF
  cat >"${self_test_dir}/completed.json" <<'EOF'
{
  "request_id": "req-2",
  "status": "completed",
  "content": "done",
  "error_message": null
}
EOF
  cat >"${self_test_dir}/max-turns.json" <<'EOF'
{
  "request_id": "req-3",
  "status": "error",
  "content": "partial work",
  "error_message": "agent stream failed: PromptError: MaxTurnError: (reached max turn limit: 250)"
}
EOF
  cat >"${self_test_dir}/provider-error.json" <<'EOF'
{
  "request_id": "req-4",
  "status": "error",
  "content": null,
  "error_message": "agent stream failed: CompletionError: ProviderError: upstream returned HTTP 500"
}
EOF
  cat >"${self_test_dir}/token-budget.json" <<'EOF'
{
  "request_id": "req-token-budget",
  "status": "error",
  "content": "partial work",
  "error_message": "agent stream failed: CompletionError: ProviderError: aggregate_token_budget_exhausted: limit=100000, used=100000 after provider call"
}
EOF
  cat >"${self_test_dir}/compaction-error.json" <<'EOF'
{
  "request_id": "req-5",
  "status": "error",
  "content": null,
  "error_message": "compaction failed: summary request rejected by provider"
}
EOF
  cat >"${self_test_dir}/compaction-provider-error.json" <<'EOF'
{
  "request_id": "req-12",
  "status": "error",
  "content": null,
  "error_message": "agent stream failed: CompletionError: ProviderError: per-turn provider-input compaction failed: compaction_provider_failure: guided and fallback output failed"
}
EOF
  cat >"${self_test_dir}/content-mentions-max-turn.json" <<'EOF'
{
  "request_id": "req-6",
  "status": "error",
  "content": "I hit MaxTurnError: in a log I was reading",
  "error_message": "agent stream failed: CompletionError: ProviderError: connection reset"
}
EOF
  cat >"${self_test_dir}/content-mentions-compaction-provider.json" <<'EOF'
{
  "request_id": "req-13",
  "status": "error",
  "content": "A log contained compaction_provider_failure: but it was not this request failure.",
  "error_message": "agent stream failed: CompletionError: ProviderError: connection reset"
}
EOF
  cat >"${self_test_dir}/unexpected-status.json" <<'EOF'
{
  "request_id": "req-7",
  "status": "interrupted",
  "content": null,
  "error_message": null
}
EOF
  cat >"${self_test_dir}/missing-status.json" <<'EOF'
{
  "request_id": "req-8",
  "content": null
}
EOF
  # Full `response wait` envelope: flat AgentResponse fields plus a nested
  # `request` object whose `failure_reason` duplicates the terminal error.
  cat >"${self_test_dir}/envelope-max-turns.json" <<'EOF'
{
  "request_id": "req-9",
  "behavior_id": "b-1",
  "session_id": "s-1",
  "status": "error",
  "content": "partial work",
  "reasoning": null,
  "error_message": "agent stream failed: PromptError: MaxTurnError: (reached max turn limit: 250)",
  "token_count": 12345,
  "completed_at": "2026-08-04T00:00:00Z",
  "request": {
    "request_id": "req-9",
    "lifecycle_state": "failed",
    "failure_reason": "agent stream failed: PromptError: MaxTurnError: (reached max turn limit: 250)"
  }
}
EOF
  # A provider error may embed upstream response text that mentions the
  # MaxTurn token; that is still an infrastructure failure.
  cat >"${self_test_dir}/provider-echoes-max-turn.json" <<'EOF'
{
  "request_id": "req-11",
  "status": "error",
  "content": null,
  "error_message": "agent stream failed: CompletionError: ProviderError: upstream mentioned MaxTurnError: (reached max turn limit: 250)"
}
EOF
  # The nested request's failure_reason must never classify on its own: here
  # it mentions MaxTurnError but the response's own error is a provider one.
  cat >"${self_test_dir}/envelope-nested-max-turn-only.json" <<'EOF'
{
  "request_id": "req-10",
  "status": "error",
  "content": null,
  "error_message": "agent stream failed: CompletionError: ProviderError: upstream returned HTTP 500",
  "request": {
    "request_id": "req-10",
    "lifecycle_state": "failed",
    "failure_reason": "child subagent hit MaxTurnError: before the provider failed"
  }
}
EOF

  expect_outcome complete completed
  expect_outcome completed completed
  expect_outcome max-turns max_turns_exhausted
  expect_outcome token-budget token_budget_exhausted
  expect_outcome provider-error agent_error
  expect_outcome compaction-error agent_error
  expect_outcome compaction-provider-error compaction_provider_error
  expect_outcome content-mentions-max-turn agent_error
  expect_outcome content-mentions-compaction-provider agent_error
  expect_outcome unexpected-status unexpected:interrupted
  expect_outcome missing-status unexpected:missing
  expect_outcome envelope-max-turns max_turns_exhausted
  expect_outcome envelope-nested-max-turn-only agent_error
  expect_outcome provider-echoes-max-turn agent_error

  for metric_expectation in \
    inference_call_count:2 \
    inference_call_pending_count:0 \
    inference_call_usage_count:2; do
    metric_key=${metric_expectation%%:*}
    metric_expected=${metric_expectation#*:}
    metric_actual=$(atif_final_metrics_extra_u64 \
      "${self_test_dir}/trajectory.json" "${metric_key}")
    if [ "${metric_actual}" != "${metric_expected}" ]; then
      printf 'FAIL: ATIF %s expected %s, got %s\n' \
        "${metric_key}" "${metric_expected}" "${metric_actual}" >&2
      failures=$((failures + 1))
    fi
  done
  if [ "$(normalize_token_budget_outcome token_budget_exhausted 0)" != "agent_error" ] || \
    [ "$(normalize_token_budget_outcome token_budget_exhausted 1)" != "token_budget_exhausted" ]; then
    printf 'FAIL: pre-dispatch token exhaustion scoreability drifted\n' >&2
    failures=$((failures + 1))
  fi

  if [ "${failures}" -ne 0 ]; then
    printf 'self-test failed: %s classification(s) wrong\n' "${failures}" >&2
    exit 1
  fi
  printf 'self-test passed\n'
  exit 0
}

if [ "${1:-}" = "self-test" ]; then
  run_self_test
fi

: "${GENTS_BINARY:=/usr/local/bin/gents}"
: "${GENTS_HOME:?GENTS_HOME is required}"
: "${GENTS_INSTRUCTION_FILE:?GENTS_INSTRUCTION_FILE is required}"
: "${GENTS_INFERENCE_URL:?GENTS_INFERENCE_URL is required}"
: "${GENTS_MODEL:?GENTS_MODEL is required}"
: "${GENTS_TOOL_ROOT:=/app}"
: "${GENTS_API_KEY:=no-key}"
: "${GENTS_TEMPERATURE:=1.0}"
: "${GENTS_TOP_P:=0.95}"
: "${GENTS_TOP_K:=}"
: "${GENTS_SEED:=}"
: "${GENTS_REASONING_EFFORT:=max}"
: "${GENTS_MAX_OUTPUT:=393216}"
: "${GENTS_MAX_TOTAL:?GENTS_MAX_TOTAL is required}"
: "${GENTS_CONTEXT_WINDOW:=458752}"
: "${GENTS_MAX_TURNS:=1000}"
: "${GENTS_RETRY_MAX_TRANSPORT:=3}"
: "${GENTS_REQUEST_TIMEOUT_SECS:=86400}"
: "${GENTS_COMMAND_TIMEOUT_SECS:=600}"
: "${GENTS_COMMAND_TIMEOUT_MAX_SECS:=3600}"
: "${GENTS_SERVER_STARTUP_TIMEOUT_SECS:=300}"
: "${GENTS_DIAGNOSTIC_TIMEOUT_SECS:=10}"
: "${GENTS_DIAGNOSTIC_HOME_MAX_BYTES:=67108864}"
: "${GENTS_SUPERVISION_POLL_SECS:=1}"
: "${GENTS_RESPONSE_WAITER_MAX_RESTARTS:=10}"
: "${GENTS_TRACE_SETTLE_TIMEOUT_SECS:=30}"
: "${GENTS_LOGS_DIR:=/logs/agent}"

for numeric_value in \
  "${GENTS_DIAGNOSTIC_TIMEOUT_SECS}" \
  "${GENTS_DIAGNOSTIC_HOME_MAX_BYTES}" \
  "${GENTS_RESPONSE_WAITER_MAX_RESTARTS}" \
  "${GENTS_TRACE_SETTLE_TIMEOUT_SECS}"; do
  case "${numeric_value}" in
    ''|*[!0-9]*|0)
      echo "diagnostic bounds must be positive integers" >&2
      exit 2
      ;;
  esac
done

logs_dir=${GENTS_LOGS_DIR}
server_log="${logs_dir}/gents-server.log"
bootstrap_server_log="${logs_dir}/gents-server-bootstrap.log"
init_log="${logs_dir}/gents-init.json"
request_log="${logs_dir}/request.json"
request_stdout="${logs_dir}/request.stdout.json"
persisted_request_log="${logs_dir}/request-persisted.json"
response_log="${logs_dir}/response.json"
response_wait_log="${logs_dir}/response-wait.stderr.log"
response_wait_attempt_log="${logs_dir}/response-wait-attempt.stderr.log"
trajectory_path="${logs_dir}/trajectory.json"
outcome_log="${logs_dir}/gents-outcome.json"
status_log="${logs_dir}/gents-status.json"
profile_log="${logs_dir}/gents-profile.json"
tools_log="${logs_dir}/gents-tools.json"
tools_explain_log="${logs_dir}/gents-tools-explain.json"
diagnostic_log="${logs_dir}/gents-diagnostic.json"
server_exit_log="${logs_dir}/gents-server-exit.json"
server_tail_log="${logs_dir}/gents-server-tail.txt"
process_tree_log="${logs_dir}/process-tree.txt"
final_status_log="${logs_dir}/gents-status-final.json"
graphql_unavailable_log="${logs_dir}/graphql-unavailable.txt"
timeline_log="${logs_dir}/partial-timeline.json"
partial_response_log="${logs_dir}/response-partial.json"
home_inventory_log="${logs_dir}/gents-home-inventory.txt"
home_archive="${logs_dir}/gents-home.tar.gz"
request_id=""
server_pid=""
waiter_pid=""
diagnostics_captured=0

mkdir -p "${logs_dir}"
test -x "${GENTS_BINARY}"
test -f "${GENTS_INSTRUCTION_FILE}"
test -d "${GENTS_TOOL_ROOT}"

case "${GENTS_MODEL}" in
  *[!A-Za-z0-9._:/-]*)
    echo "GENTS_MODEL contains unsupported characters" >&2
    exit 2
    ;;
esac

case "${GENTS_REASONING_EFFORT}" in
  low|high|max) ;;
  *)
    echo "GENTS_REASONING_EFFORT must be one of: low, high, max" >&2
    exit 2
    ;;
esac

case "${GENTS_SEED}" in
  ''|*[!0-9]*)
    if [ -n "${GENTS_SEED}" ]; then
      echo "GENTS_SEED must be a non-negative integer" >&2
      exit 2
    fi
    ;;
esac

# GENTS_MAX_TURNS is interpolated into the outcome document as a JSON number,
# so leading zeros are as invalid as non-digits.
case "${GENTS_MAX_TURNS}" in
  ''|*[!0-9]*|0?*)
    echo "GENTS_MAX_TURNS must be a non-negative integer without leading zeros" >&2
    exit 2
    ;;
esac

case "${GENTS_MAX_TOTAL}" in
  ''|*[!0-9]*|0|0?*)
    echo "GENTS_MAX_TOTAL must be a positive integer without leading zeros" >&2
    exit 2
    ;;
esac

"${GENTS_BINARY}" init \
  --home "${GENTS_HOME}" \
  --agent-name harbor-gents \
  --backend-preset vllm \
  --inference-url "${GENTS_INFERENCE_URL}" \
  --openai-wire-api chat-completions \
  --api-key "${GENTS_API_KEY}" \
  --model-name "${GENTS_MODEL}" \
  --max-concurrent 1 \
  --max-queue-depth 1 \
  --tool-package write \
  --tool-root "${GENTS_TOOL_ROOT}" \
  >"${init_log}"

start_server() {
  "${GENTS_BINARY}" server \
    --home "${GENTS_HOME}" \
    --http-addr 127.0.0.1 \
    --http-port 9191 \
    --tool-ceiling readwrite \
    --tool-root "${GENTS_TOOL_ROOT}" \
    --command-timeout-secs "${GENTS_COMMAND_TIMEOUT_SECS}" \
    --command-timeout-max-secs "${GENTS_COMMAND_TIMEOUT_MAX_SECS}" \
    >>"${server_log}" 2>&1 &
  server_pid=$!
}

process_is_running() {
  process_pid=$1
  kill -0 "${process_pid}" >/dev/null 2>&1 || return 1
  process_state=$(ps -o stat= -p "${process_pid}" 2>/dev/null || true)
  case "${process_state}" in
    Z*|*' Z'*) return 1 ;;
    *) return 0 ;;
  esac
}

run_bounded() {
  bounded_output=$1
  bounded_timeout=$2
  shift 2
  "$@" >"${bounded_output}" 2>&1 &
  bounded_pid=$!
  bounded_elapsed=0
  while process_is_running "${bounded_pid}"; do
    if [ "${bounded_elapsed}" -ge "${bounded_timeout}" ]; then
      kill "${bounded_pid}" >/dev/null 2>&1 || true
      sleep 1
      kill -KILL "${bounded_pid}" >/dev/null 2>&1 || true
      wait "${bounded_pid}" >/dev/null 2>&1 || true
      printf '\ndiagnostic command exceeded %ss and was terminated\n' "${bounded_timeout}" >>"${bounded_output}"
      return 124
    fi
    sleep 1
    bounded_elapsed=$((bounded_elapsed + 1))
  done
  bounded_status=0
  wait "${bounded_pid}" || bounded_status=$?
  return "${bounded_status}"
}

stop_waiter() {
  if [ -z "${waiter_pid}" ]; then
    return
  fi
  if process_is_running "${waiter_pid}"; then
    kill "${waiter_pid}" >/dev/null 2>&1 || true
    waiter_stop_attempt=0
    while process_is_running "${waiter_pid}" && [ "${waiter_stop_attempt}" -lt 20 ]; do
      sleep 0.1
      waiter_stop_attempt=$((waiter_stop_attempt + 1))
    done
    if process_is_running "${waiter_pid}"; then
      kill -KILL "${waiter_pid}" >/dev/null 2>&1 || true
    fi
  fi
  wait "${waiter_pid}" >/dev/null 2>&1 || true
  waiter_pid=""
}

record_server_exit() {
  server_wait_status=0
  wait "${server_pid}" || server_wait_status=$?
  if [ "${server_wait_status}" -gt 128 ]; then
    server_signal=$((server_wait_status - 128))
    printf '{\n  "status": "signal",\n  "signal": %s,\n  "wait_status": %s\n}\n' \
      "${server_signal}" "${server_wait_status}" >"${server_exit_log}"
    server_exit_description="signal ${server_signal}"
  else
    printf '{\n  "status": "exit",\n  "exit_code": %s,\n  "wait_status": %s\n}\n' \
      "${server_wait_status}" "${server_wait_status}" >"${server_exit_log}"
    server_exit_description="exit code ${server_wait_status}"
  fi
  if [ -n "${request_id}" ]; then
    printf '{\n  "outcome": "runtime_server_lost",\n  "response_status": "unavailable",\n  "max_turns": %s,\n  "max_total_tokens": %s,\n  "request_id": "%s"\n}\n' \
      "${GENTS_MAX_TURNS}" "${GENTS_MAX_TOTAL}" "${request_id}" >"${outcome_log}"
  fi
  server_pid=""
}

capture_diagnostics() {
  diagnostic_reason=$1
  if [ "${diagnostics_captured}" = "1" ]; then
    return
  fi
  diagnostics_captured=1

  tail -200 "${server_log}" >"${server_tail_log}" 2>&1 || true
  ps -ef >"${process_tree_log}" 2>&1 || printf 'process-tree snapshot unavailable\n' >"${process_tree_log}"

  graphql_available=false
  if run_bounded "${final_status_log}" "${GENTS_DIAGNOSTIC_TIMEOUT_SECS}" \
    "${GENTS_BINARY}" status --home "${GENTS_HOME}"; then
    graphql_available=true
    rm -f "${graphql_unavailable_log}"
  else
    printf 'GraphQL unavailable while capturing diagnostics; see %s\n' "${final_status_log}" \
      >"${graphql_unavailable_log}"
  fi

  if [ -n "${request_id}" ]; then
    if run_bounded "${partial_response_log}" "${GENTS_DIAGNOSTIC_TIMEOUT_SECS}" \
      "${GENTS_BINARY}" response show --home "${GENTS_HOME}" --request-id "${request_id}"; then
      if [ ! -s "${response_log}" ]; then
        cp "${partial_response_log}" "${response_log}"
      fi
    fi
    run_bounded "${timeline_log}" "${GENTS_DIAGNOSTIC_TIMEOUT_SECS}" \
      "${GENTS_BINARY}" trace timeline --home "${GENTS_HOME}" --request-id "${request_id}" || true
    if [ ! -s "${trajectory_path}" ]; then
      run_bounded "${logs_dir}/partial-atif-export.log" "${GENTS_DIAGNOSTIC_TIMEOUT_SECS}" \
        "${GENTS_BINARY}" trace project \
        --home "${GENTS_HOME}" \
        --request-id "${request_id}" \
        --projection atif \
        --format native-json \
        --output-file "${trajectory_path}" || true
    fi
  fi

  if [ -d "${GENTS_HOME}" ]; then
    find "${GENTS_HOME}" -type f -exec ls -ln {} \; 2>&1 |
      sed -n '1,2000p' >"${home_inventory_log}" || true
    home_kib=$(du -sk "${GENTS_HOME}" 2>/dev/null | sed -n 's/^[[:space:]]*\([0-9][0-9]*\).*/\1/p' || true)
    case "${home_kib}" in
      ''|*[!0-9]*) home_kib=0 ;;
    esac
    home_archive_limit_kib=$(((GENTS_DIAGNOSTIC_HOME_MAX_BYTES + 1023) / 1024))
    if [ "${home_kib}" -le "${home_archive_limit_kib}" ]; then
      home_parent=$(dirname "${GENTS_HOME}")
      home_name=$(basename "${GENTS_HOME}")
      run_bounded "${logs_dir}/gents-home-archive.log" "${GENTS_DIAGNOSTIC_TIMEOUT_SECS}" \
        tar -czf "${home_archive}" -C "${home_parent}" "${home_name}" || true
    else
      printf 'GENTS_HOME is %s KiB; archive limit is %s bytes\n' \
        "${home_kib}" "${GENTS_DIAGNOSTIC_HOME_MAX_BYTES}" \
        >"${logs_dir}/gents-home-archive-skipped.txt"
    fi
  fi

  printf '{\n  "reason": "%s",\n  "request_id": "%s",\n  "graphql_available": %s,\n  "home_archive_limit_bytes": %s\n}\n' \
    "${diagnostic_reason}" "${request_id}" "${graphql_available}" \
    "${GENTS_DIAGNOSTIC_HOME_MAX_BYTES}" >"${diagnostic_log}"
}

wait_for_server_ready() {
  server_ready=0
  attempt=0
  while [ "${attempt}" -lt "${GENTS_SERVER_STARTUP_TIMEOUT_SECS}" ]; do
    # The persisted status file can still describe the bootstrap server for a
    # brief window after restart. Requiring the new process's serving marker
    # prevents a concurrent home-opening CLI (notably `tools explain`) from
    # racing the restarted server for RocksDB's exclusive LOCK.
    if grep -qF 'gents server is running with' "${server_log}" &&
      "${GENTS_BINARY}" status --home "${GENTS_HOME}" >"${status_log}" 2>/dev/null &&
      grep -q '"process_state": "ready"' "${status_log}" &&
      grep -q '"behavior_readiness": "ready"' "${status_log}"; then
      server_ready=1
      break
    fi
    if ! kill -0 "${server_pid}" >/dev/null 2>&1; then
      echo "Gents server exited during startup" >&2
      tail -200 "${server_log}" >&2 || true
      exit 1
    fi
    sleep 1
    attempt=$((attempt + 1))
  done
  if [ "${server_ready}" != "1" ]; then
    echo "Gents server did not become ready in ${GENTS_SERVER_STARTUP_TIMEOUT_SECS}s" >&2
    tail -200 "${server_log}" >&2 || true
    exit 1
  fi
}

: >"${server_log}"
profile_id=$(sed -n 's/^[[:space:]]*"inference_profile_id": "\([^"]*\)",*$/\1/p' "${init_log}" | head -1)
if [ -z "${profile_id}" ]; then
  echo "Gents init output did not contain inference_profile_id" >&2
  exit 1
fi
agent_did=$(sed -n 's/^[[:space:]]*"agent_did": "\([^"]*\)",*$/\1/p' "${init_log}" | head -1)
tool_selection_id=$(sed -n 's/^[[:space:]]*"tool_selection_id": "\([^"]*\)",*$/\1/p' "${init_log}" | head -1)
behavior_id=$(sed -n 's/^[[:space:]]*"default_behavior_id": "\([^"]*\)",*$/\1/p' "${init_log}" | head -1)
if [ -z "${agent_did}" ] || [ -z "${tool_selection_id}" ] || [ -z "${behavior_id}" ]; then
  echo "Gents init output did not contain agent_did, default_behavior_id, and tool_selection_id" >&2
  exit 1
fi

configure_profile() {
  profile_configured=0
  profile_attempt=0
  profile_attempt_limit=$((GENTS_SERVER_STARTUP_TIMEOUT_SECS * 10))
  while [ "${profile_attempt}" -lt "${profile_attempt_limit}" ]; do
    if "${GENTS_BINARY}" config profile set \
      --graphql http://127.0.0.1:9191/api/v0/graphql \
      --profile-id "${profile_id}" \
      --context-window "${GENTS_CONTEXT_WINDOW}" \
      --max-output-tokens "${GENTS_MAX_OUTPUT}" \
      --max-turns "${GENTS_MAX_TURNS}" \
      --temperature "${GENTS_TEMPERATURE}" \
      --top-p "${GENTS_TOP_P}" \
      --reasoning-effort "${GENTS_REASONING_EFFORT}" \
      --stream-liveness-timeout-secs "${GENTS_REQUEST_TIMEOUT_SECS}" \
      --deadline-duration-secs "${GENTS_REQUEST_TIMEOUT_SECS}" \
      --retry-max-transport "${GENTS_RETRY_MAX_TRANSPORT}" \
      >"${profile_log}" 2>/dev/null; then
      profile_configured=1
      break
    fi
    if ! kill -0 "${server_pid}" >/dev/null 2>&1; then
      echo "Gents server exited before its inference profile could be configured" >&2
      tail -200 "${server_log}" >&2 || true
      exit 1
    fi
    sleep 0.1
    profile_attempt=$((profile_attempt + 1))
  done
  if [ "${profile_configured}" != "1" ]; then
    echo "Gents GraphQL did not accept the inference profile within ${GENTS_SERVER_STARTUP_TIMEOUT_SECS}s" >&2
    tail -200 "${server_log}" >&2 || true
    exit 1
  fi
}

configure_tools() {
  "${GENTS_BINARY}" config tools set \
    --graphql http://127.0.0.1:9191/api/v0/graphql \
    --agent-did "${agent_did}" \
    --selection-id "${tool_selection_id}" \
    --enable-file-tools true \
    --file-tools-mode ReadWrite \
    --file-tool-root "${GENTS_TOOL_ROOT}" \
    --enable-bash true \
    --bash-mode Unrestricted \
    --command-execution-policy unrestricted \
    --enable-meta-tools false \
    --backgroundable-tool-name bash_unrestricted \
    --enable-memory false \
    --enable-session-history-tool false \
    --enable-context-budget false \
    --enable-defra-query false \
    --subagent-spawn-enabled false \
    --orchestration-enabled false \
    --subagent-steering-enabled false \
    --subagent-background-enabled false \
    --subagent-allow-cross-deployment false \
    >"${tools_log}"
}

start_server

cleanup() {
  exit_code=$?
  trap - EXIT INT TERM
  stop_waiter
  if [ "${exit_code}" -ne 0 ] && [ "${diagnostics_captured}" != "1" ]; then
    capture_diagnostics "runner_exit"
  elif [ -n "${request_id}" ] && [ ! -s "${trajectory_path}" ]; then
    capture_diagnostics "partial_trace"
  fi
  if [ -n "${server_pid}" ]; then
    kill "${server_pid}" >/dev/null 2>&1 || true
    wait "${server_pid}" >/dev/null 2>&1 || true
  fi
  exit "${exit_code}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

configure_profile
configure_tools

# Configure through GraphQL before requiring behavior readiness. This also
# bootstraps binaries whose schema materializes an omitted nullable string as
# an empty value. Restarting makes the persisted profile part of the startup
# snapshot before any benchmark request can exist.
kill "${server_pid}" >/dev/null 2>&1 || true
wait "${server_pid}" || true
mv "${server_log}" "${bootstrap_server_log}"
: >"${server_log}"
start_server
wait_for_server_ready

"${GENTS_BINARY}" tools explain \
  --home "${GENTS_HOME}" \
  --behavior-id "${behavior_id}" \
  >"${tools_explain_log}"

metadata=$(printf '{"harness":"harbor","model_name":"%s"}' "${GENTS_MODEL}")
set -- \
  request submit
set -- "$@" \
  --home "${GENTS_HOME}" \
  --content-file "${GENTS_INSTRUCTION_FILE}" \
  --temperature "${GENTS_TEMPERATURE}" \
  --top-p "${GENTS_TOP_P}" \
  --max-tokens "${GENTS_MAX_OUTPUT}" \
  --max-total-tokens "${GENTS_MAX_TOTAL}" \
  --metadata "${metadata}" \
  --valid-until none \
  --no-wait \
  --output-file "${request_log}"
if [ -n "${GENTS_TOP_K}" ]; then
  set -- "$@" --top-k "${GENTS_TOP_K}"
fi
if [ -n "${GENTS_SEED}" ]; then
  set -- "$@" --seed "${GENTS_SEED}"
fi
"${GENTS_BINARY}" "$@" >"${request_stdout}"

request_id=$(sed -n 's/^[[:space:]]*"request_id": "\([^"]*\)",*$/\1/p' "${request_log}" | head -1)
if [ -z "${request_id}" ]; then
  echo "Gents request output did not contain request_id" >&2
  tail -200 "${request_log}" >&2 || true
  exit 1
fi

"${GENTS_BINARY}" request show \
  --home "${GENTS_HOME}" \
  --request-id "${request_id}" \
  --output json \
  >"${persisted_request_log}"

: >"${response_wait_log}"
waiter_restart_count=0
while :; do
  : >"${response_log}"
  : >"${response_wait_attempt_log}"
  "${GENTS_BINARY}" response wait \
    --home "${GENTS_HOME}" \
    --request-id "${request_id}" \
    --timeout-secs "${GENTS_REQUEST_TIMEOUT_SECS}" \
    --poll-secs 1 \
    >"${response_log}" 2>"${response_wait_attempt_log}" &
  waiter_pid=$!

  while process_is_running "${waiter_pid}"; do
    if ! process_is_running "${server_pid}"; then
      record_server_exit
      stop_waiter
      capture_diagnostics "server_lost_during_request"
      echo "Gents server exited during active request (${server_exit_description}); waiter cancelled; diagnostics=${diagnostic_log}" >&2
      exit 70
    fi
    sleep "${GENTS_SUPERVISION_POLL_SECS}"
  done

  waiter_status=0
  wait "${waiter_pid}" || waiter_status=$?
  waiter_pid=""
  if ! process_is_running "${server_pid}"; then
    record_server_exit
    capture_diagnostics "server_lost_during_request"
    echo "Gents server exited during active request (${server_exit_description}); diagnostics=${diagnostic_log}" >&2
    exit 70
  fi

  transient_waiter_failure=0
  if grep -Eq \
    'posting GraphQL to|reading GraphQL response|decoding GraphQL response|GraphQL request retries exhausted' \
    "${response_wait_attempt_log}"; then
    transient_waiter_failure=1
  fi
  printf '%s\n' "--- response waiter attempt $((waiter_restart_count + 1)) (status ${waiter_status}) ---" \
    >>"${response_wait_log}"
  cat "${response_wait_attempt_log}" >>"${response_wait_log}"

  if [ "${waiter_status}" -eq 0 ]; then
    rm -f "${response_wait_attempt_log}"
    break
  fi
  if [ "${transient_waiter_failure}" = "1" ] &&
    [ "${waiter_restart_count}" -lt "${GENTS_RESPONSE_WAITER_MAX_RESTARTS}" ]; then
    waiter_restart_count=$((waiter_restart_count + 1))
    echo "Gents response waiter exhausted transient GraphQL retries; restarting (${waiter_restart_count}/${GENTS_RESPONSE_WAITER_MAX_RESTARTS})" \
      | tee -a "${response_wait_log}" >&2
    sleep 1
    continue
  fi

  rm -f "${response_wait_attempt_log}"
  echo "Gents response waiter exited with status ${waiter_status}" >&2
  exit "${waiter_status}"
done

# Persist terminal classification before trace settling. If projection cannot
# reopen the store, diagnostics still retain the response-derived outcome.
response_status=$(sed -n 's/^[[:space:]]*"status": "\([^"]*\)",*$/\1/p' "${response_log}" | head -1)
outcome=$(classify_response "${response_log}")
printf '{\n  "outcome": "%s",\n  "response_status": "%s",\n  "max_turns": %s,\n  "max_total_tokens": %s,\n  "request_id": "%s"\n}\n' \
  "${outcome}" "${response_status:-missing}" "${GENTS_MAX_TURNS}" "${GENTS_MAX_TOTAL}" "${request_id}" \
  >"${outcome_log}"

trajectory_candidate="${trajectory_path}.pending"
trace_settle_elapsed=0
trace_settled=0
inference_call_count=""
inference_call_usage_count=""
while [ "${trace_settle_elapsed}" -lt "${GENTS_TRACE_SETTLE_TIMEOUT_SECS}" ]; do
  rm -f "${trajectory_candidate}"
  if "${GENTS_BINARY}" trace project \
    --home "${GENTS_HOME}" \
    --request-id "${request_id}" \
    --projection atif \
    --format native-json \
    --output-file "${trajectory_candidate}" && \
    [ -s "${trajectory_candidate}" ]; then
    inference_call_count=$(atif_final_metrics_extra_u64 \
      "${trajectory_candidate}" inference_call_count)
    pending_call_count=$(atif_final_metrics_extra_u64 \
      "${trajectory_candidate}" inference_call_pending_count)
    inference_call_usage_count=$(atif_final_metrics_extra_u64 \
      "${trajectory_candidate}" inference_call_usage_count)
    if [ -n "${inference_call_count}" ] && \
      [ -n "${inference_call_usage_count}" ] && \
      [ "${pending_call_count:-missing}" = "0" ]; then
      mv "${trajectory_candidate}" "${trajectory_path}"
      trace_settled=1
      break
    fi
  fi
  sleep 1
  trace_settle_elapsed=$((trace_settle_elapsed + 1))
done
rm -f "${trajectory_candidate}"
if [ "${trace_settled}" != "1" ]; then
  echo "Gents inference-call usage did not settle within ${GENTS_TRACE_SETTLE_TIMEOUT_SECS}s" >&2
  exit 1
fi

classification_reason=""
normalized_outcome=$(normalize_token_budget_outcome \
  "${outcome}" "${inference_call_usage_count:-0}")
if [ "${normalized_outcome}" != "${outcome}" ]; then
  classification_reason="aggregate token budget was exhausted before any provider call retained chargeable usage"
fi
outcome=${normalized_outcome}

# `response wait` exits successfully after any terminal response, including a
# provider/runtime failure. Do not let Harbor run the verifier against an
# untouched task filesystem and record that infrastructure failure as a model
# zero. The exceptions are explicit agent-budget exhaustion (turns or aggregate
# provider tokens): the workspace holds real work, so return control to Harbor
# and let the verifier score it. Preserve the response and trajectory above in
# every case; genuine failures stay agent exceptions so they can be retried or
# recovered separately.
if [ -n "${classification_reason}" ]; then
  printf '{\n  "outcome": "%s",\n  "response_status": "%s",\n  "max_turns": %s,\n  "max_total_tokens": %s,\n  "request_id": "%s",\n  "classification_reason": "%s"\n}\n' \
    "${outcome}" "${response_status:-missing}" "${GENTS_MAX_TURNS}" "${GENTS_MAX_TOTAL}" "${request_id}" "${classification_reason}" \
    >"${outcome_log}"
fi
case "${outcome}" in
  completed)
    printf 'gents request %s completed; trajectory=%s\n' "${request_id}" "${trajectory_path}"
    ;;
  max_turns_exhausted)
    echo "Gents request ${request_id} exhausted its ${GENTS_MAX_TURNS}-turn budget; returning the workspace for verification" >&2
    sed -n '/^[[:space:]]*"error_message":/p' "${response_log}" >&2 || true
    printf 'gents request %s reached the %s-turn limit; trajectory=%s\n' \
      "${request_id}" "${GENTS_MAX_TURNS}" "${trajectory_path}"
    ;;
  token_budget_exhausted)
    echo "Gents request ${request_id} exhausted its ${GENTS_MAX_TOTAL}-token aggregate budget; returning the workspace for verification" >&2
    sed -n '/^[[:space:]]*"error_message":/p' "${response_log}" >&2 || true
    printf 'gents request %s reached the %s-token aggregate limit; trajectory=%s\n' \
      "${request_id}" "${GENTS_MAX_TOTAL}" "${trajectory_path}"
    ;;
  compaction_provider_error)
    echo "Gents request ${request_id} terminated because both guided and strict fallback compaction failed" >&2
    sed -n '/^[[:space:]]*"error_message":/p' "${response_log}" >&2 || true
    exit 1
    ;;
  agent_error)
    echo "Gents request ${request_id} terminated with an error response" >&2
    sed -n '/^[[:space:]]*"error_message":/p' "${response_log}" >&2 || true
    exit 1
    ;;
  *)
    echo "Gents request ${request_id} returned unexpected response status: ${response_status:-missing}" >&2
    exit 1
    ;;
esac
