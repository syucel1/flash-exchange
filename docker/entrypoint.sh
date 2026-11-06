#!/bin/sh
# FlashEx container entrypoint: first argument selects the role, remaining arguments are passed through.
#   media-driver    [driver.properties ...]   default: config/media-driver.properties
#   matching-engine [engine.properties]       default: config/engine.properties
#   demo-trader     [demo.properties]         default: config/demo-ipc.properties
#   health          probe the engine HTTP endpoint (exit 0 when /health returns 200)
#   driver-health   check that the media driver's CnC file exists
#   anything else   executed as a command
set -eu

APP_JAR="${FLASHEX_HOME:-/opt/flashex}/lib/flashex.jar"
CONFIG_DIR="${FLASHEX_HOME:-/opt/flashex}/config"
ROLE="${1:-matching-engine}"
[ "$#" -gt 0 ] && shift

case "$ROLE" in
  media-driver)
    [ "$#" -eq 0 ] && set -- "$CONFIG_DIR/media-driver.properties"
    # shellcheck disable=SC2086
    exec java $JAVA_OPTS -cp "$APP_JAR" io.aeron.driver.MediaDriver "$@"
    ;;
  matching-engine)
    [ "$#" -eq 0 ] && set -- "$CONFIG_DIR/engine.properties"
    # shellcheck disable=SC2086
    exec java $JAVA_OPTS -cp "$APP_JAR" com.flashexchange.engine.MatchingEngineMain "$@"
    ;;
  demo-trader)
    [ "$#" -eq 0 ] && set -- "$CONFIG_DIR/demo-ipc.properties"
    # shellcheck disable=SC2086
    exec java $JAVA_OPTS -cp "$APP_JAR" com.flashexchange.engine.tools.DemoTrader "$@"
    ;;
  health)
    PORT="${FLASHEX_HTTP_PORT:-8080}"
    exec bash -c "exec 3<>/dev/tcp/127.0.0.1/$PORT && printf 'GET /health HTTP/1.0\r\n\r\n' >&3 && head -n 1 <&3 | grep -q ' 200 '"
    ;;
  driver-health)
    test -f "${AERON_DIR:-/dev/shm/aeron}/cnc.dat"
    ;;
  *)
    exec "$ROLE" "$@"
    ;;
esac
