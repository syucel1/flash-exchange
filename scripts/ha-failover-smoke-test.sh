#!/usr/bin/env bash
# Failover drill on the docker-compose HA profile: arbiter + engine-a + engine-b + demo trader.
#   1. wait until one engine is PRIMARY, the other STANDBY and traffic flows
#   2. kill -9 the primary container
#   3. check the standby is promoted (and report how long it took), traffic continues
#   4. restart the killed node and check it rejoins as STANDBY of the new primary
#
#   scripts/ha-failover-smoke-test.sh [--keep]
# Requires the image to be built already (docker compose build, or FLASHEX_IMAGE pointing at a pulled image).
set -euo pipefail

KEEP=false
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=true ;;
    *) echo "unknown argument: $arg" >&2; exit 64 ;;
  esac
done

TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
MAX_FAILOVER_MS="${MAX_FAILOVER_MS:-1000}"
COMPOSE=(docker compose --profile ha)
declare -A PORT=([engine-a]=8091 [engine-b]=8092)

cleanup() {
  if [ "$KEEP" = false ]; then
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

json_value() { # json_value <json> <key>  (first occurrence, numbers or strings)
  sed -n "s/.*\"$2\":\"\{0,1\}\([A-Za-z0-9_-]*\).*/\1/p" <<<"$1" | head -n 1
}

status_of() { curl -fsS "http://localhost:${PORT[$1]}/status" 2>/dev/null || true; }
role_of() { json_value "$(status_of "$1")" role; }
now_ms() { date +%s%3N; }

wait_for() { # wait_for <description> <command...>
  local what="$1" deadline=$((SECONDS + TIMEOUT_SECONDS))
  shift
  until "$@"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "FAIL: timed out waiting for $what" >&2
      curl -fsS http://localhost:8090/status >&2 || true
      "${COMPOSE[@]}" logs --tail 80 >&2
      exit 1
    fi
    sleep 0.2
  done
}

pair_ready() {
  local a b
  a="$(role_of engine-a)"; b="$(role_of engine-b)"
  { [ "$a" = PRIMARY ] && [ "$b" = STANDBY ]; } || { [ "$a" = STANDBY ] && [ "$b" = PRIMARY ]; }
}
primary() { if [ "$(role_of engine-a)" = PRIMARY ]; then echo engine-a; else echo engine-b; fi; }
commands_on() { json_value "$(status_of "$1")" commandsReceived; }
traffic_on() { local c; c="$(commands_on "$1")"; [ -n "$c" ] && [ "$c" -ge "$2" ]; }

echo "starting HA stack..."
"${COMPOSE[@]}" up -d arbiter engine-a engine-b demo-trader-ha
wait_for "a primary/standby pair" pair_ready
OLD="$(primary)"
NEW=$([ "$OLD" = engine-a ] && echo engine-b || echo engine-a)
echo "primary=$OLD standby=$NEW"
wait_for "traffic on $OLD" traffic_on "$OLD" 200

echo "killing $OLD (SIGKILL)..."
CONTAINER="$("${COMPOSE[@]}" ps -q "$OLD")"
docker kill -s KILL "$CONTAINER" >/dev/null
KILLED_AT="$(now_ms)"   # SIGKILL has been delivered when docker kill returns
wait_for "$NEW promoted" bash -c "[ \"\$(curl -fsS http://localhost:${PORT[$NEW]}/status 2>/dev/null | \
  sed -n 's/.*\"role\":\"\([A-Z]*\)\".*/\1/p')\" = PRIMARY ]"
OBSERVED_AT="$(now_ms)"
SINCE_PROMOTION="$(json_value "$(curl -fsS http://localhost:8090/status)" msSinceLastPromotion)"
# the arbiter reports how long ago it promoted; subtract it to remove the polling delay of this script
FAILOVER_MS=$(( $(now_ms) - KILLED_AT - ${SINCE_PROMOTION:-0} ))
echo "OK: $NEW promoted ${FAILOVER_MS}ms after the kill (seen by this script after $((OBSERVED_AT - KILLED_AT))ms)"
if [ "$FAILOVER_MS" -gt "$MAX_FAILOVER_MS" ]; then
  echo "FAIL: failover took longer than ${MAX_FAILOVER_MS}ms" >&2
  exit 1
fi
BEFORE="$(commands_on "$NEW")"
wait_for "traffic on the new primary" traffic_on "$NEW" $(( ${BEFORE:-0} + 200 ))
echo "OK: traffic continues on $NEW"

echo "restarting $OLD..."
"${COMPOSE[@]}" start "$OLD" >/dev/null
wait_for "$OLD to rejoin as standby" bash -c "[ \"\$(curl -fsS http://localhost:${PORT[$OLD]}/status 2>/dev/null | \
  sed -n 's/.*\"role\":\"\([A-Z]*\)\".*/\1/p')\" = STANDBY ]"
EXCEPTIONS="$(json_value "$(status_of "$NEW")" eventExceptions)"
if [ "${EXCEPTIONS:-0}" != "0" ]; then
  echo "FAIL: $NEW eventExceptions=$EXCEPTIONS" >&2
  exit 1
fi
echo "OK: $OLD rejoined as STANDBY; arbiter: $(curl -fsS http://localhost:8090/status)"
echo "PASS: HA failover drill"
