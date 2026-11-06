#!/usr/bin/env bash
# Smoke test of the docker-compose stack: starts it, waits until the demo trader has driven real traffic through
# the engine (IPC stack and, with --udp, the UDP stack), checks for rejects/exceptions and tears everything down.
#
#   scripts/compose-smoke-test.sh [--udp] [--keep]
# Requires the image to be built already (docker compose build, or FLASHEX_IMAGE pointing at a pulled image).
set -euo pipefail

PROFILE_ARGS=()
CHECK_UDP=false
KEEP=false
for arg in "$@"; do
  case "$arg" in
    --udp) PROFILE_ARGS=(--profile udp); CHECK_UDP=true ;;
    --keep) KEEP=true ;;
    *) echo "unknown argument: $arg" >&2; exit 64 ;;
  esac
done

MIN_COMMANDS="${MIN_COMMANDS:-500}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
COMPOSE=(docker compose "${PROFILE_ARGS[@]}")

cleanup() {
  if [ "$KEEP" = false ]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

json_number() { # json_number <json> <key>
  sed -n "s/.*\"$2\":\([0-9-]*\).*/\1/p" <<<"$1"
}

check_engine() { # check_engine <name> <port>
  local name="$1" port="$2" deadline=$((SECONDS + TIMEOUT_SECONDS)) status commands
  echo "waiting for $name to process at least $MIN_COMMANDS commands..."
  while true; do
    status="$(curl -fsS "http://localhost:$port/status" 2>/dev/null || true)"
    commands="$(json_number "$status" commandsReceived)"
    if [ -n "$commands" ] && [ "$commands" -ge "$MIN_COMMANDS" ]; then
      break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "FAIL: $name processed ${commands:-0} commands within ${TIMEOUT_SECONDS}s" >&2
      "${COMPOSE[@]}" logs --tail 50 >&2
      exit 1
    fi
    sleep 2
  done
  local exceptions malformed
  exceptions="$(json_number "$status" eventExceptions)"
  malformed="$(json_number "$status" malformedMessages)"
  if [ "$exceptions" != "0" ] || [ "$malformed" != "0" ]; then
    echo "FAIL: $name eventExceptions=$exceptions malformedMessages=$malformed" >&2
    exit 1
  fi
  echo "OK: $name commandsReceived=$commands eventExceptions=0 malformedMessages=0"
}

check_demo() { # check_demo <service>
  local service="$1" line
  line="$("${COMPOSE[@]}" logs --no-log-prefix "$service" | grep '^orders=' | tail -n 1 || true)"
  if [ -z "$line" ]; then
    sleep 12
    line="$("${COMPOSE[@]}" logs --no-log-prefix "$service" | grep '^orders=' | tail -n 1 || true)"
  fi
  echo "$service: $line"
  if ! grep -q ' rejected=0 ' <<<"$line" || grep -q ' trades=0 ' <<<"$line"; then
    echo "FAIL: $service reported rejects or no trades" >&2
    exit 1
  fi
}

"${COMPOSE[@]}" up -d --no-build --wait --wait-timeout "$TIMEOUT_SECONDS" media-driver matching-engine
"${COMPOSE[@]}" up -d --no-build
check_engine matching-engine 8080
check_demo demo-trader
if [ "$CHECK_UDP" = true ]; then
  check_engine matching-engine-udp 8081
  check_demo demo-trader-udp
fi
echo "smoke test passed"
