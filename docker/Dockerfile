# syntax=docker/dockerfile:1.7
#
# FlashEx runtime image. One image, several roles (see entrypoint.sh):
#   media-driver     standalone Aeron media driver (shared by co-located containers over /dev/shm)
#   matching-engine  the matching engine (com.flashexchange.engine.MatchingEngineMain)
#   demo-trader      load generator / demo client (com.flashexchange.engine.tools.DemoTrader)
#
# The application jar is built by Maven first (./mvnw package) and copied in; CI builds it in the same workflow.
#   docker build -f docker/Dockerfile -t flash-exchange:local .

ARG BASE_IMAGE=eclipse-temurin:21-jre-noble
# hadolint ignore=DL3006
FROM ${BASE_IMAGE}

ARG APP_VERSION=0.1.0-SNAPSHOT
ARG REVISION=local
LABEL org.opencontainers.image.title="flash-exchange" \
      org.opencontainers.image.description="FlashEx matching engine, Aeron media driver and demo trader" \
      org.opencontainers.image.source="https://github.com/syucel1/flash-exchange" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${APP_VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

RUN groupadd --system --gid 10001 flashex \
 && useradd --system --uid 10001 --gid flashex --home-dir /opt/flashex --no-create-home --shell /usr/sbin/nologin flashex \
 && mkdir -p /opt/flashex/lib /opt/flashex/config /opt/flashex/bin

WORKDIR /opt/flashex
# application files stay root-owned and read-only for the runtime user
COPY --chmod=0755 docker/entrypoint.sh /opt/flashex/bin/entrypoint.sh
COPY --chmod=0644 docker/config/ /opt/flashex/config/
COPY --chmod=0644 matching-engine/target/matching-engine-*-all.jar /opt/flashex/lib/flashex.jar

# Agrona 2.x needs jdk.internal.misc; Aeron tunes sockets through sun.nio.ch. Applies to every java invocation.
ENV JDK_JAVA_OPTIONS="--add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED" \
    JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -Xms256m -Xmx256m" \
    FLASHEX_HOME=/opt/flashex

# 8080: engine HTTP (/health, /status); 40101-40301/udp: engine Aeron endpoints in UDP mode
EXPOSE 8080 40101/udp 40201/udp 40301/udp

USER 10001:10001
ENTRYPOINT ["/opt/flashex/bin/entrypoint.sh"]
CMD ["matching-engine"]
