package com.flashexchange.engine.ha;

import com.flashexchange.engine.config.HaConfig;
import com.flashexchange.protocol.ArbiterCommandFlyweight;
import com.flashexchange.protocol.ArbiterCommandType;
import com.flashexchange.protocol.MessageHeader;
import com.flashexchange.protocol.NodeHeartbeatFlyweight;
import com.flashexchange.protocol.NodeRole;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * The witness that decides which engine node is primary. It is deliberately simple and holds almost no state: the
 * current epoch and primary (persisted), plus the latest heartbeat of each node.
 * <p>
 * Rules, evaluated every duty cycle:
 * <ol>
 *   <li>No primary yet: promote the preferred node (first in the list) once it is up and pending, or the first
 *       pending node after the start-up grace period.</li>
 *   <li>Primary alive: renew its lease every third of the lease duration; tell pending nodes to FOLLOW it; tell a
 *       standby that lost the replication stream to FOLLOW again; FENCE any other node claiming to be primary. A
 *       primary that restarted (reports PENDING) is promoted again with a new epoch.</li>
 *   <li>Primary silent for the primary timeout <em>and</em> its last lease expired (plus a guard): promote the
 *       standby, but only if it is caught up and has handled at least everything the old primary reported as
 *       released. Otherwise wait: promoting a standby that misses released output would lose fills.</li>
 * </ol>
 * Each promotion increments the epoch. Every command carries the epoch, and nodes ignore stale ones.
 */
public final class Arbiter implements Agent
{
    private static final System.Logger LOGGER = System.getLogger(Arbiter.class.getName());
    private static final long COMMAND_RESEND_NS = TimeUnit.MILLISECONDS.toNanos(200);

    private final ArbiterConfig config;
    private final Subscription heartbeats;
    private final NodeView[] nodes;
    private final LongSupplier clock;
    private final Path stateFile;
    private final FragmentHandler heartbeatHandler = this::onHeartbeat;
    private final NodeHeartbeatFlyweight heartbeat = new NodeHeartbeatFlyweight();
    private final ArbiterCommandFlyweight command = new ArbiterCommandFlyweight();
    private final UnsafeBuffer commandBuffer =
        new UnsafeBuffer(ByteBuffer.allocateDirect(ArbiterCommandFlyweight.ENCODED_LENGTH));
    private final long leaseNs;
    private final long primaryTimeoutNs;
    private final long leaseGuardNs;
    private final long startNs;

    private final AtomicLong epoch = new AtomicLong();
    private volatile int primaryId;
    private final AtomicLong promotions = new AtomicLong();
    private final AtomicLong failovers = new AtomicLong();
    private volatile String lastDecision = "starting";
    private long lastLeaseNs;
    private long lastPromoteNs;
    private boolean waitingLogged;

    /**
     * Create the arbiter.
     *
     * @param config configuration.
     * @param aeron  Aeron client.
     * @param clock  monotonic nanosecond clock.
     */
    public Arbiter(final ArbiterConfig config, final Aeron aeron, final LongSupplier clock)
    {
        this.config = config;
        this.clock = clock;
        this.stateFile = Path.of(config.stateFile());
        this.heartbeats = aeron.addSubscription(
            HaConfig.udp(config.listenHost(), config.listenPort()), HaConfig.HEARTBEAT_STREAM_ID);
        this.nodes = new NodeView[config.nodes().size()];
        for (int i = 0; i < nodes.length; i++)
        {
            final ArbiterConfig.Node node = config.nodes().get(i);
            nodes[i] = new NodeView(node.id(), aeron.addPublication(
                HaConfig.udp(node.host(), node.controlPort()), HaConfig.ARBITER_COMMAND_STREAM_ID));
        }
        this.leaseNs = TimeUnit.MILLISECONDS.toNanos(config.leaseDurationMs());
        this.primaryTimeoutNs = TimeUnit.MILLISECONDS.toNanos(config.primaryTimeoutMs());
        this.leaseGuardNs = TimeUnit.MILLISECONDS.toNanos(config.leaseGuardMs());
        this.startNs = clock.getAsLong();
        loadState();
        // a lease may have been granted just before a restart of the arbiter
        this.lastLeaseNs = startNs;
    }

    private void loadState()
    {
        if (!Files.exists(stateFile))
        {
            return;
        }
        final Properties p = new Properties();
        try (InputStream in = Files.newInputStream(stateFile))
        {
            p.load(in);
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
        epoch.set(Long.parseLong(p.getProperty("epoch", "0")));
        primaryId = Integer.parseInt(p.getProperty("primary", "0"));
        LOGGER.log(System.Logger.Level.INFO, "arbiter state: epoch " + epoch.get() + ", primary " + primaryId);
    }

    private void saveState()
    {
        final Properties p = new Properties();
        p.setProperty("epoch", Long.toString(epoch.get()));
        p.setProperty("primary", Integer.toString(primaryId));
        try
        {
            final Path parent = stateFile.toAbsolutePath().getParent();
            if (parent == null)
            {
                throw new IOException("state file has no parent directory: " + stateFile);
            }
            Files.createDirectories(parent);
            final Path tmp = parent.resolve(stateFile.getFileName() + ".tmp");
            try (OutputStream out = Files.newOutputStream(tmp))
            {
                p.store(out, "FlashEx arbiter state");
            }
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public String roleName()
    {
        return "arbiter";
    }

    @Override
    public int doWork()
    {
        final int work = heartbeats.poll(heartbeatHandler, 64);
        decide(clock.getAsLong());
        return work;
    }

    private void onHeartbeat(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (length < NodeHeartbeatFlyweight.ENCODED_LENGTH ||
            MessageHeader.templateId(buffer, offset) != NodeHeartbeatFlyweight.TEMPLATE_ID)
        {
            return;
        }
        heartbeat.wrapForDecode(buffer, offset);
        final NodeView node = node(heartbeat.nodeId());
        if (node == null)
        {
            return;
        }
        node.seen = true;
        node.lastSeenNs = clock.getAsLong();
        node.role = heartbeat.role();
        node.epoch = heartbeat.epoch();
        node.lastSequence = heartbeat.lastSequence();
        node.releasedSequence = heartbeat.releasedSequence();
        node.caughtUp = heartbeat.caughtUp() != 0;
        node.peerVisible = heartbeat.peerVisible() != 0;
    }

    private NodeView node(final int id)
    {
        for (final NodeView node : nodes)
        {
            if (node.id == id)
            {
                return node;
            }
        }
        return null;
    }

    private boolean alive(final NodeView node, final long nowNs)
    {
        return node != null && node.seen && nowNs - node.lastSeenNs < primaryTimeoutNs;
    }

    void decide(final long nowNs)
    {
        if (primaryId == 0)
        {
            electInitial(nowNs);
            return;
        }

        final NodeView primary = node(primaryId);
        if (alive(primary, nowNs) && primary.role != NodeRole.FENCED)
        {
            waitingLogged = false;
            superviseLivePrimary(primary, nowNs);
        }
        else
        {
            failover(primary, nowNs);
        }
    }

    private void electInitial(final long nowNs)
    {
        NodeView candidate = null;
        final NodeView preferred = nodes[0];
        if (alive(preferred, nowNs) && preferred.role == NodeRole.PENDING)
        {
            candidate = preferred;
        }
        else if (nowNs - startNs > TimeUnit.MILLISECONDS.toNanos(config.startupGraceMs()))
        {
            for (final NodeView node : nodes)
            {
                if (alive(node, nowNs) && node.role == NodeRole.PENDING)
                {
                    candidate = node;
                    break;
                }
            }
        }
        if (candidate != null)
        {
            promote(candidate, nowNs, "initial election");
        }
    }

    private void superviseLivePrimary(final NodeView primary, final long nowNs)
    {
        if (primary.role == NodeRole.PENDING)
        {
            if (primary.epoch >= epoch.get())
            {
                // the primary restarted: it must lead under a new epoch so consumers can de-duplicate
                promote(primary, nowNs, "primary restarted");
            }
            else if (nowNs - primary.lastCommandNs > COMMAND_RESEND_NS)
            {
                send(primary, ArbiterCommandType.PROMOTE, nowNs);
                lastLeaseNs = nowNs;
            }
            return;
        }
        if (primary.role == NodeRole.STANDBY)
        {
            // planned switchover (the recorded primary is a standby, e.g. after editing the state file): promote
            // it once any lease held by the current primary has run out
            if (nowNs - lastLeaseNs > leaseNs + leaseGuardNs && nowNs - primary.lastCommandNs > COMMAND_RESEND_NS)
            {
                send(primary, ArbiterCommandType.PROMOTE, nowNs);
            }
            fenceOthers(primary, nowNs);
            return;
        }
        if (primary.role == NodeRole.PRIMARY && primary.epoch < epoch.get())
        {
            if (nowNs - primary.lastCommandNs > COMMAND_RESEND_NS)
            {
                send(primary, ArbiterCommandType.PROMOTE, nowNs);
                lastLeaseNs = nowNs;
            }
            return;
        }
        if (primary.role == NodeRole.PRIMARY && nowNs - lastLeaseNs >= leaseNs / 3)
        {
            send(primary, ArbiterCommandType.LEASE, nowNs);
            lastLeaseNs = nowNs;
        }

        fenceOthers(primary, nowNs);
        final boolean primaryReady = primary.role == NodeRole.PRIMARY && primary.epoch == epoch.get();
        for (final NodeView node : nodes)
        {
            if (node == primary || !alive(node, nowNs) || nowNs - node.lastCommandNs < COMMAND_RESEND_NS)
            {
                continue;
            }
            if (primaryReady && (node.role == NodeRole.PENDING ||
                (node.role == NodeRole.STANDBY && (node.epoch != epoch.get() || !node.peerVisible))))
            {
                send(node, ArbiterCommandType.FOLLOW, nowNs);
            }
        }
    }

    private void fenceOthers(final NodeView primary, final long nowNs)
    {
        for (final NodeView node : nodes)
        {
            if (node != primary && alive(node, nowNs) && node.role == NodeRole.PRIMARY &&
                nowNs - node.lastCommandNs >= COMMAND_RESEND_NS)
            {
                send(node, ArbiterCommandType.FENCE, nowNs);
            }
        }
    }

    private void failover(final NodeView primary, final long nowNs)
    {
        if (nowNs - lastLeaseNs <= leaseNs + leaseGuardNs)
        {
            return;
        }
        final long released = primary == null ? -1 : primary.releasedSequence;
        for (final NodeView node : nodes)
        {
            if (node != primary && alive(node, nowNs) && node.role == NodeRole.STANDBY && node.caughtUp &&
                node.epoch == epoch.get() && node.lastSequence >= released)
            {
                failovers.incrementAndGet();
                promote(node, nowNs, "primary " + primaryId + " silent; standby has sequence " +
                    node.lastSequence + " >= released " + released);
                return;
            }
        }
        if (!waitingLogged)
        {
            waitingLogged = true;
            lastDecision = "primary " + primaryId + " silent and no eligible standby";
            LOGGER.log(System.Logger.Level.WARNING, lastDecision);
        }
    }

    private void promote(final NodeView node, final long nowNs, final String reason)
    {
        epoch.incrementAndGet();
        primaryId = node.id;
        saveState();
        promotions.incrementAndGet();
        lastPromoteNs = nowNs;
        lastLeaseNs = nowNs;
        lastDecision = "promoted node " + node.id + " at epoch " + epoch.get() + " (" + reason + ")";
        LOGGER.log(System.Logger.Level.INFO, lastDecision);
        send(node, ArbiterCommandType.PROMOTE, nowNs);
    }

    private void send(final NodeView node, final ArbiterCommandType type, final long nowNs)
    {
        command.wrapForEncode(commandBuffer, 0)
            .epoch(epoch.get())
            .leaseDurationMs(config.leaseDurationMs())
            .targetNodeId(node.id)
            .primaryNodeId(primaryId)
            .command(type);
        node.publication.offer(commandBuffer, 0, ArbiterCommandFlyweight.ENCODED_LENGTH);
        node.lastCommandNs = nowNs;
    }

    /**
     * Current epoch.
     *
     * @return epoch.
     */
    public long epoch()
    {
        return epoch.get();
    }

    /**
     * Current primary.
     *
     * @return node id, 0 before the first election.
     */
    public int primaryId()
    {
        return primaryId;
    }

    /**
     * Promotions decided (including the initial election).
     *
     * @return count.
     */
    public long promotions()
    {
        return promotions.get();
    }

    /**
     * Status for the {@code /status} endpoint.
     *
     * @return status entries.
     */
    public Map<String, Object> status()
    {
        final long nowNs = clock.getAsLong();
        final Map<String, Object> status = new LinkedHashMap<>();
        status.put("epoch", epoch.get());
        status.put("primary", primaryId);
        status.put("promotions", promotions.get());
        status.put("failovers", failovers.get());
        status.put("lastDecision", lastDecision);
        status.put("msSinceLastPromotion",
            lastPromoteNs == 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(nowNs - lastPromoteNs));
        final List<Map<String, Object>> nodeStatus = new ArrayList<>();
        for (final NodeView node : nodes)
        {
            final Map<String, Object> n = new LinkedHashMap<>();
            n.put("id", node.id);
            n.put("alive", alive(node, nowNs));
            n.put("role", node.role == null ? "UNKNOWN" : node.role.name());
            n.put("epoch", node.epoch);
            n.put("lastSequence", node.lastSequence);
            n.put("releasedSequence", node.releasedSequence);
            n.put("caughtUp", node.caughtUp);
            n.put("peerVisible", node.peerVisible);
            nodeStatus.add(n);
        }
        status.put("nodes", nodeStatus);
        return status;
    }

    @Override
    public void onClose()
    {
        heartbeats.close();
        for (final NodeView node : nodes)
        {
            node.publication.close();
        }
    }

    private static final class NodeView
    {
        final int id;
        final Publication publication;
        boolean seen;
        long lastSeenNs;
        long lastCommandNs = Long.MIN_VALUE / 2;
        NodeRole role;
        long epoch;
        long lastSequence = -1;
        long releasedSequence = -1;
        boolean caughtUp;
        boolean peerVisible;

        NodeView(final int id, final Publication publication)
        {
            this.id = id;
            this.publication = publication;
        }
    }
}
