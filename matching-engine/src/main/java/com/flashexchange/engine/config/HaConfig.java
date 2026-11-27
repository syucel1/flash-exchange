package com.flashexchange.engine.config;

import java.util.Locale;
import java.util.Properties;

/**
 * Primary/standby settings. Both nodes run the same configuration apart from their own and their peer's
 * addresses. Requires the journal and a remotely reachable archive control channel.
 *
 * @param enabled             run as an HA node under an arbiter (otherwise the engine is STANDALONE).
 * @param nodeId              unique node id (&gt; 0); also the archive id.
 * @param releaseMode         SYNC (wait for the standby's ack) or ASYNC.
 * @param selfHost            address peers and the arbiter use to reach this node.
 * @param replicationPort     UDP port the primary's archive replays the journal to (stream 101).
 * @param ackPort             UDP port this node receives replication acks on when primary (stream 102).
 * @param controlPort         UDP port this node receives arbiter commands on (stream 9002).
 * @param peerHost            address of the other node.
 * @param peerArchivePort     archive control port of the other node.
 * @param peerAckPort         ack port of the other node.
 * @param arbiterHost         arbiter address.
 * @param arbiterPort         arbiter heartbeat port (stream 9001).
 * @param heartbeatIntervalMs period of heartbeats to the arbiter.
 * @param ackTimeoutMs        SYNC mode: how long a release waits for the standby before degrading to ASYNC.
 */
public record HaConfig(
    boolean enabled,
    int nodeId,
    ReleaseMode releaseMode,
    String selfHost,
    int replicationPort,
    int ackPort,
    int controlPort,
    String peerHost,
    int peerArchivePort,
    int peerAckPort,
    String arbiterHost,
    int arbiterPort,
    long heartbeatIntervalMs,
    long ackTimeoutMs)
{
    /** Replication stream (archive replay of the primary's journal to the standby). */
    public static final int REPLICATION_STREAM_ID = 101;

    /** Replication acknowledgement stream (standby to primary). */
    public static final int ACK_STREAM_ID = 102;

    /** Node heartbeats to the arbiter. */
    public static final int HEARTBEAT_STREAM_ID = 9001;

    /** Arbiter commands to a node. */
    public static final int ARBITER_COMMAND_STREAM_ID = 9002;

    /**
     * Validate.
     */
    public HaConfig
    {
        if (nodeId < 1)
        {
            throw new IllegalArgumentException("nodeId must be >= 1: " + nodeId);
        }
        if (releaseMode == null)
        {
            throw new IllegalArgumentException("releaseMode is required");
        }
        if (heartbeatIntervalMs < 1 || ackTimeoutMs < 1)
        {
            throw new IllegalArgumentException("heartbeat interval and ack timeout must be >= 1ms");
        }
    }

    /**
     * HA disabled (standalone engine with node id 1).
     *
     * @return defaults.
     */
    public static HaConfig disabled()
    {
        return new HaConfig(false, 1, ReleaseMode.SYNC, "localhost", 40111, 40112, 40113, "localhost", 8010,
            40112, "localhost", 40901, 50, 200);
    }

    /**
     * Copy with the node identity and ports changed.
     *
     * @param id              node id.
     * @param host            own address.
     * @param replication     replication port.
     * @param ack             ack port.
     * @param control         arbiter command port.
     * @return copy.
     */
    public HaConfig withSelf(final int id, final String host, final int replication, final int ack, final int control)
    {
        return new HaConfig(enabled, id, releaseMode, host, replication, ack, control, peerHost, peerArchivePort,
            peerAckPort, arbiterHost, arbiterPort, heartbeatIntervalMs, ackTimeoutMs);
    }

    /**
     * Copy with the peer changed.
     *
     * @param host        peer address.
     * @param archivePort peer archive control port.
     * @param ack         peer ack port.
     * @return copy.
     */
    public HaConfig withPeer(final String host, final int archivePort, final int ack)
    {
        return new HaConfig(enabled, nodeId, releaseMode, selfHost, replicationPort, ackPort, controlPort, host,
            archivePort, ack, arbiterHost, arbiterPort, heartbeatIntervalMs, ackTimeoutMs);
    }

    /**
     * Copy with HA enabled and the arbiter endpoint and release mode set.
     *
     * @param host arbiter address.
     * @param port arbiter port.
     * @param mode release mode.
     * @return copy.
     */
    public HaConfig withArbiter(final String host, final int port, final ReleaseMode mode)
    {
        return new HaConfig(true, nodeId, mode, selfHost, replicationPort, ackPort, controlPort, peerHost,
            peerArchivePort, peerAckPort, host, port, heartbeatIntervalMs, ackTimeoutMs);
    }

    /**
     * Copy with the timing changed.
     *
     * @param heartbeatMs heartbeat period.
     * @param ackMs       ack timeout.
     * @return copy.
     */
    public HaConfig withTiming(final long heartbeatMs, final long ackMs)
    {
        return new HaConfig(enabled, nodeId, releaseMode, selfHost, replicationPort, ackPort, controlPort, peerHost,
            peerArchivePort, peerAckPort, arbiterHost, arbiterPort, heartbeatMs, ackMs);
    }

    /**
     * Read the {@code ha.*} keys.
     *
     * @param p      properties.
     * @param prefix key prefix.
     * @return configuration.
     */
    public static HaConfig fromProperties(final Properties p, final String prefix)
    {
        final HaConfig d = disabled();
        return new HaConfig(
            Boolean.parseBoolean(get(p, prefix, "ha.enabled", String.valueOf(d.enabled))),
            Integer.parseInt(get(p, prefix, "ha.nodeId", String.valueOf(d.nodeId))),
            ReleaseMode.valueOf(get(p, prefix, "ha.mode", d.releaseMode.name()).toUpperCase(Locale.ROOT)),
            get(p, prefix, "ha.self.host", d.selfHost),
            Integer.parseInt(get(p, prefix, "ha.self.replicationPort", String.valueOf(d.replicationPort))),
            Integer.parseInt(get(p, prefix, "ha.self.ackPort", String.valueOf(d.ackPort))),
            Integer.parseInt(get(p, prefix, "ha.self.controlPort", String.valueOf(d.controlPort))),
            get(p, prefix, "ha.peer.host", d.peerHost),
            Integer.parseInt(get(p, prefix, "ha.peer.archivePort", String.valueOf(d.peerArchivePort))),
            Integer.parseInt(get(p, prefix, "ha.peer.ackPort", String.valueOf(d.peerAckPort))),
            get(p, prefix, "ha.arbiter.host", d.arbiterHost),
            Integer.parseInt(get(p, prefix, "ha.arbiter.port", String.valueOf(d.arbiterPort))),
            Long.parseLong(get(p, prefix, "ha.heartbeatIntervalMs", String.valueOf(d.heartbeatIntervalMs))),
            Long.parseLong(get(p, prefix, "ha.ackTimeoutMs", String.valueOf(d.ackTimeoutMs))));
    }

    private static String get(final Properties p, final String prefix, final String key, final String def)
    {
        return DurabilityConfig.get(p, prefix, key, def);
    }

    /**
     * UDP channel for an endpoint.
     *
     * @param host address.
     * @param port port.
     * @return channel URI.
     */
    public static String udp(final String host, final int port)
    {
        return "aeron:udp?endpoint=" + host + ":" + port;
    }
}
