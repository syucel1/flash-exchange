package com.flashexchange.engine.node;

import com.flashexchange.engine.config.DurabilityConfig;
import com.flashexchange.engine.config.EngineConfig;
import com.flashexchange.engine.config.HaConfig;
import com.flashexchange.engine.durability.EngineArchive;
import com.flashexchange.engine.durability.Journal;
import com.flashexchange.engine.durability.RecordingInfo;
import com.flashexchange.engine.durability.SnapshotImage;
import com.flashexchange.engine.durability.SnapshotStore;
import com.flashexchange.engine.ha.ReplicationLink;
import com.flashexchange.engine.pipeline.MessagePublisher;
import com.flashexchange.engine.pipeline.PublisherEventHandler;
import com.flashexchange.protocol.ArbiterCommandFlyweight;
import com.flashexchange.protocol.ArbiterCommandType;
import com.flashexchange.protocol.MessageHeader;
import com.flashexchange.protocol.NodeHeartbeatFlyweight;
import com.flashexchange.protocol.NodeRole;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-threaded conductor of an engine node and the only producer into the Disruptor ring. It owns the node's
 * role state machine:
 * <pre>
 *   STANDALONE  (HA disabled)  recover locally, accept commands
 *   PENDING ──PROMOTE──▶ PRIMARY   recover from the local snapshot + journal, accept commands while the lease holds
 *   PENDING ──FOLLOW───▶ STANDBY   fetch the primary's snapshot, follow its journal (archive replay on 101), ack 102
 *   STANDBY ──PROMOTE──▶ PRIMARY   apply what was received, announce the new epoch, re-publish retained output
 *   PRIMARY ──FENCE/FOLLOW──▶ FENCED  drop output, stop; the process restarts and follows the new primary
 * </pre>
 * Roles are assigned by the arbiter (stream 9002); the node reports its state in heartbeats (stream 9001). A
 * primary only accepts commands and releases unacknowledged output while it holds a lease granted by the arbiter,
 * which never promotes the standby before that lease has expired.
 */
public final class EngineConductor implements Agent
{
    private static final System.Logger LOGGER = System.getLogger(EngineConductor.class.getName());
    private static final long FOLLOW_RETRY_NS = TimeUnit.MILLISECONDS.toNanos(500);

    private final EngineConfig config;
    private final DurabilityConfig durabilityConfig;
    private final HaConfig ha;
    private final Aeron aeron;
    private final Subscription commandSubscription;
    private final MessagePublisher executionPublisher;
    private final MessagePublisher marketDataPublisher;
    private final EngineArchive archive;
    private final EpochFile epochFile;
    private final Runnable onFenced;
    private final ReplicationLink link;
    private final Publication heartbeatPublication;
    private final Subscription arbiterSubscription;
    private final Subscription ackSubscription;
    private final Publication ackPublication;
    private final String replicationChannel;

    private final UnsafeBuffer heartbeatBuffer =
        new UnsafeBuffer(ByteBuffer.allocateDirect(NodeHeartbeatFlyweight.ENCODED_LENGTH));
    private final NodeHeartbeatFlyweight heartbeat = new NodeHeartbeatFlyweight();
    private final ArbiterCommandFlyweight arbiterCommand = new ArbiterCommandFlyweight();
    private final FragmentHandler arbiterHandler = this::onArbiterMessage;
    private final FragmentHandler replicationHandler = this::onReplicatedEntry;
    private final FragmentHandler discardHandler = (buffer, offset, length, header) -> { };

    private volatile EngineCore core;
    private volatile NodeRole role;
    private volatile long epoch;
    private volatile long followPosition = -1;
    private volatile boolean peerVisible;
    private final AtomicLong promotions = new AtomicLong();
    private final AtomicLong followReconnects = new AtomicLong();
    private volatile long lastPromotionNs;

    private AeronArchive remoteArchive;
    private Subscription replaySubscription;
    private long followRecordingId = -1;
    private int replaySessionId;
    private long lastFollowAttemptNs;
    private long nextHeartbeatNs;
    private long nextStateSnapshotNs;
    private long lastSnapshotSequence = Long.MIN_VALUE;
    private boolean leaseLost;

    /**
     * Create the conductor. Call {@link #startStandalone()} when HA is disabled; otherwise run it as an agent and
     * wait for the arbiter.
     *
     * @param config              engine configuration.
     * @param aeron               Aeron client.
     * @param commandSubscription command stream (1001).
     * @param executionPublisher  private event output.
     * @param marketDataPublisher market data output.
     * @param archive             local archive, or {@code null} when the journal is disabled.
     * @param onFenced            called (on the conductor thread) when the node is fenced and must restart.
     */
    public EngineConductor(
        final EngineConfig config,
        final Aeron aeron,
        final Subscription commandSubscription,
        final MessagePublisher executionPublisher,
        final MessagePublisher marketDataPublisher,
        final EngineArchive archive,
        final Runnable onFenced)
    {
        this.config = config;
        this.durabilityConfig = config.durability();
        this.ha = config.ha();
        this.aeron = aeron;
        this.commandSubscription = commandSubscription;
        this.executionPublisher = executionPublisher;
        this.marketDataPublisher = marketDataPublisher;
        this.archive = archive;
        this.onFenced = onFenced;
        this.epochFile = durabilityConfig.journalEnabled() ?
            new EpochFile(Path.of(durabilityConfig.dataDirectory(), "node-epoch")) : null;
        this.replicationChannel = HaConfig.udp(ha.selfHost(), ha.replicationPort());

        if (ha.enabled())
        {
            this.role = NodeRole.PENDING;
            this.heartbeatPublication = aeron.addPublication(
                HaConfig.udp(ha.arbiterHost(), ha.arbiterPort()), HaConfig.HEARTBEAT_STREAM_ID);
            this.arbiterSubscription = aeron.addSubscription(
                HaConfig.udp(ha.selfHost(), ha.controlPort()), HaConfig.ARBITER_COMMAND_STREAM_ID);
            this.ackSubscription = aeron.addSubscription(
                HaConfig.udp(ha.selfHost(), ha.ackPort()), HaConfig.ACK_STREAM_ID);
            this.ackPublication = aeron.addPublication(
                HaConfig.udp(ha.peerHost(), ha.peerAckPort()), HaConfig.ACK_STREAM_ID);
            this.link = new ReplicationLink(
                ha.releaseMode(), ha.ackTimeoutMs(), ha.nodeId(), ackSubscription, ackPublication, System::nanoTime);
            this.epoch = epochFile.read(0);
        }
        else
        {
            this.role = NodeRole.STANDALONE;
            this.heartbeatPublication = null;
            this.arbiterSubscription = null;
            this.ackSubscription = null;
            this.ackPublication = null;
            this.link = null;
        }
        this.nextStateSnapshotNs = System.nanoTime() + snapshotIntervalNs();
    }

    /**
     * Start without HA: recover from the local snapshot and journal (when enabled) and accept commands.
     */
    public void startStandalone()
    {
        if (archive == null)
        {
            core = EngineCore.start(config, commandSubscription, executionPublisher, marketDataPublisher, null, null,
                null, null);
            return;
        }
        epoch = epochFile.read(0) + 1;
        epochFile.write(epoch);
        recoverLocally(NodeRole.STANDALONE);
        LOGGER.log(System.Logger.Level.INFO, "standalone engine recovered, epoch " + epoch + ", sequence " +
            core.sequencer().lastSequence());
    }

    private void recoverLocally(final NodeRole newRole)
    {
        final Journal journal = archive.openJournal(durabilityConfig.journalTermLength());
        final long journalEnd = journal.position();
        final SnapshotImage snapshot =
            SnapshotStore.latest(archive.client(), aeron, EngineArchive.IPC, EngineArchive.REPLAY_STREAM_ID);
        final long journalStart = journalStartPosition(journal.recordingId());
        long replayFrom = journalStart;
        if (snapshot != null)
        {
            if (snapshot.journalRecordingId() == journal.recordingId())
            {
                replayFrom = Math.max(snapshot.journalPosition(), journalStart);
            }
            else
            {
                LOGGER.log(System.Logger.Level.WARNING, "snapshot " + snapshot.recordingId() +
                    " refers to journal " + snapshot.journalRecordingId() + ", not " + journal.recordingId() +
                    "; restoring the snapshot without replay");
                replayFrom = journalEnd;
            }
        }
        final EngineCore newCore = EngineCore.start(config, commandSubscription, executionPublisher,
            marketDataPublisher, archive, journal, link, snapshot);
        newCore.ingress().publishRoleChange(newRole, epoch);
        newCore.replayJournal(archive, journal.recordingId(), replayFrom, journalEnd);
        role = newRole;
        core = newCore;
        lastSnapshotSequence = snapshot != null ? snapshot.snapshotSequence() : Long.MIN_VALUE;
        if (snapshot == null || newCore.sequencer().lastSequence() != snapshot.snapshotSequence() ||
            newRole == NodeRole.PRIMARY)
        {
            takeStateSnapshot();
        }
    }

    private long journalStartPosition(final long recordingId)
    {
        for (final RecordingInfo info : archive.recordings(EngineArchive.JOURNAL_STREAM_ID))
        {
            if (info.recordingId() == recordingId)
            {
                return info.startPosition();
            }
        }
        return 0;
    }

    @Override
    public String roleName()
    {
        return "engine-conductor";
    }

    @Override
    public int doWork()
    {
        final long nowNs = System.nanoTime();
        int work = 0;
        if (arbiterSubscription != null)
        {
            work += arbiterSubscription.poll(arbiterHandler, 8);
        }

        final EngineCore current = core;
        switch (role)
        {
            case STANDALONE:
                work += current.ingress().doWork(commandBudget(current));
                break;

            case PRIMARY:
                work += link.pollAcks(current.sequencer().lastSequence());
                if (link.leaseValid())
                {
                    leaseLost = false;
                    work += current.ingress().doWork(commandBudget(current));
                }
                else if (!leaseLost)
                {
                    leaseLost = true;
                    LOGGER.log(System.Logger.Level.WARNING, "lease expired: holding commands until renewed");
                }
                break;

            case STANDBY:
                work += current.ingress().discard();
                if (replaySubscription != null)
                {
                    work += replaySubscription.poll(replicationHandler, config.fragmentLimit());
                    peerVisible = replaySubscription.imageBySessionId(replaySessionId) != null;
                }
                break;

            default:
                work += commandSubscription.poll(discardHandler, config.fragmentLimit());
                break;
        }

        work += checkStateSnapshot(nowNs);
        if (heartbeatPublication != null && nowNs - nextHeartbeatNs >= 0)
        {
            nextHeartbeatNs = nowNs + TimeUnit.MILLISECONDS.toNanos(ha.heartbeatIntervalMs());
            sendHeartbeat();
            work++;
        }
        return work;
    }

    private int commandBudget(final EngineCore current)
    {
        final long everyCommands = durabilityConfig.stateSnapshotIntervalCommands();
        if (everyCommands <= 0 || !current.isDurable())
        {
            return Integer.MAX_VALUE;
        }
        final long sinceSnapshot = current.sequencer().lastSequence() - lastSnapshotSequence;
        return (int)Math.max(1, Math.min(Integer.MAX_VALUE, everyCommands - sinceSnapshot));
    }

    private long snapshotIntervalNs()
    {
        final long ms = durabilityConfig.stateSnapshotIntervalMs();
        return ms > 0 ? TimeUnit.MILLISECONDS.toNanos(ms) : Long.MAX_VALUE / 4;
    }

    private int checkStateSnapshot(final long nowNs)
    {
        final EngineCore current = core;
        if (current == null || !current.isDurable() ||
            (role != NodeRole.STANDALONE && role != NodeRole.PRIMARY && role != NodeRole.STANDBY))
        {
            return 0;
        }
        final long sequence = current.sequencer().lastSequence();
        final long everyCommands = durabilityConfig.stateSnapshotIntervalCommands();
        final boolean due = nowNs - nextStateSnapshotNs >= 0 ||
            (everyCommands > 0 && sequence - lastSnapshotSequence >= everyCommands);
        if (!due)
        {
            return 0;
        }
        nextStateSnapshotNs = nowNs + snapshotIntervalNs();
        if (sequence == lastSnapshotSequence)
        {
            return 0;
        }
        takeStateSnapshot();
        return 1;
    }

    private void takeStateSnapshot()
    {
        final EngineCore current = core;
        if (current != null && current.isDurable())
        {
            current.snapshot();
            lastSnapshotSequence = current.sequencer().lastSequence();
        }
    }

    /**
     * Schedule a final snapshot before an orderly shutdown. Must be called after the conductor agent stopped (the
     * caller becomes the ring producer).
     */
    public void onShutdown()
    {
        final NodeRole current = role;
        if (current == NodeRole.STANDALONE || current == NodeRole.PRIMARY || current == NodeRole.STANDBY)
        {
            final EngineCore c = core;
            if (c != null && c.isDurable() && c.sequencer().lastSequence() != lastSnapshotSequence)
            {
                takeStateSnapshot();
            }
        }
    }

    private void sendHeartbeat()
    {
        final EngineCore current = core;
        final PublisherEventHandler publisher = current == null ? null : current.pipeline().publisher();
        final boolean standby = role == NodeRole.STANDBY;
        heartbeat.wrapForEncode(heartbeatBuffer, 0)
            .epoch(epoch)
            .lastSequence(publisher == null ? -1 : publisher.lastHandledEngineSequence())
            .releasedSequence(publisher == null ? -1 : publisher.lastReleasedEngineSequence())
            .timestamp(System.currentTimeMillis())
            .nodeId(ha.nodeId())
            .role(role)
            .caughtUp((byte)(standby && followRecordingId >= 0 ? 1 : 0))
            .peerVisible((byte)(peerVisible ? 1 : 0));
        heartbeatPublication.offer(heartbeatBuffer, 0, NodeHeartbeatFlyweight.ENCODED_LENGTH);
        if (standby && publisher != null)
        {
            link.sendKeepAlive(publisher.lastHandledEngineSequence());
        }
    }

    private void onArbiterMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (length < ArbiterCommandFlyweight.ENCODED_LENGTH ||
            MessageHeader.templateId(buffer, offset) != ArbiterCommandFlyweight.TEMPLATE_ID)
        {
            return;
        }
        arbiterCommand.wrapForDecode(buffer, offset);
        if (arbiterCommand.targetNodeId() != ha.nodeId())
        {
            return;
        }
        final ArbiterCommandType type = arbiterCommand.command();
        final long commandEpoch = arbiterCommand.epoch();
        final long leaseNs = TimeUnit.MILLISECONDS.toNanos(arbiterCommand.leaseDurationMs());
        if (type == null)
        {
            return;
        }
        switch (type)
        {
            case PROMOTE:
                onPromote(commandEpoch, leaseNs);
                break;

            case LEASE:
                if (role == NodeRole.PRIMARY && commandEpoch == epoch)
                {
                    link.leaseUntil(System.nanoTime() + leaseNs);
                }
                break;

            case FOLLOW:
                onFollow(commandEpoch);
                break;

            case FENCE:
                if (role == NodeRole.PRIMARY && commandEpoch > epoch)
                {
                    fence("fenced by the arbiter at epoch " + commandEpoch);
                }
                break;

            default:
                break;
        }
    }

    private void onPromote(final long newEpoch, final long leaseNs)
    {
        if (newEpoch <= epoch && role == NodeRole.PRIMARY)
        {
            return;
        }
        if (newEpoch < epoch || role == NodeRole.FENCED)
        {
            return;
        }
        final long previous = epoch;
        epoch = newEpoch;
        epochFile.write(newEpoch);
        link.epoch(newEpoch);
        link.leaseRequired(true);
        link.leaseUntil(System.nanoTime() + leaseNs);
        lastPromotionNs = System.nanoTime();
        switch (role)
        {
            case PENDING:
                recoverLocally(NodeRole.PRIMARY);
                break;

            case STANDBY:
                drainReplication();
                closeFollower();
                core.ingress().publishRoleChange(NodeRole.PRIMARY, newEpoch);
                role = NodeRole.PRIMARY;
                takeStateSnapshot();
                break;

            default:
                core.ingress().publishRoleChange(NodeRole.PRIMARY, newEpoch);
                role = NodeRole.PRIMARY;
                break;
        }
        promotions.incrementAndGet();
        LOGGER.log(System.Logger.Level.INFO, "node " + ha.nodeId() + " promoted to PRIMARY, epoch " + previous +
            " -> " + newEpoch + ", sequence " + core.sequencer().lastSequence());
    }

    private void onFollow(final long newEpoch)
    {
        if (newEpoch < epoch)
        {
            return;
        }
        switch (role)
        {
            case PENDING:
                if (System.nanoTime() - lastFollowAttemptNs > FOLLOW_RETRY_NS)
                {
                    lastFollowAttemptNs = System.nanoTime();
                    bootstrapStandby(newEpoch);
                }
                break;

            case STANDBY:
                epoch = newEpoch;
                link.epoch(newEpoch);
                if (!peerVisible && System.nanoTime() - lastFollowAttemptNs > FOLLOW_RETRY_NS)
                {
                    lastFollowAttemptNs = System.nanoTime();
                    reconnectReplication();
                }
                break;

            case PRIMARY:
                if (newEpoch > epoch)
                {
                    fence("another node is primary at epoch " + newEpoch);
                }
                break;

            default:
                break;
        }
    }

    private void bootstrapStandby(final long newEpoch)
    {
        AeronArchive remote = null;
        try
        {
            remote = EngineArchive.connectRemote(aeron, ha.peerHost(), ha.peerArchivePort(), ha.selfHost());
            final SnapshotImage snapshot =
                SnapshotStore.latest(remote, aeron, replicationChannel, EngineArchive.SNAPSHOT_STREAM_ID);
            if (snapshot == null)
            {
                LOGGER.log(System.Logger.Level.INFO, "primary has no snapshot yet; will retry");
                CloseHelper.quietClose(remote);
                return;
            }
            archive.purgeAll();
            final Journal journal = archive.openJournal(durabilityConfig.journalTermLength());
            epoch = newEpoch;
            epochFile.write(newEpoch);
            link.epoch(newEpoch);
            link.leaseRequired(false);
            final EngineCore newCore = EngineCore.start(config, commandSubscription, executionPublisher,
                marketDataPublisher, archive, journal, link, snapshot);
            newCore.ingress().publishRoleChange(NodeRole.STANDBY, newEpoch);
            core = newCore;
            role = NodeRole.STANDBY;
            takeStateSnapshot();
            remoteArchive = remote;
            replaySubscription = aeron.addSubscription(replicationChannel, HaConfig.REPLICATION_STREAM_ID);
            followRecordingId = snapshot.journalRecordingId();
            followPosition = snapshot.journalPosition();
            startReplay();
            LOGGER.log(System.Logger.Level.INFO, "node " + ha.nodeId() + " following primary from sequence " +
                snapshot.snapshotSequence() + " (journal " + followRecordingId + " @ " + followPosition + ")");
        }
        catch (final RuntimeException ex)
        {
            if (core == null)
            {
                CloseHelper.quietClose(remote);
                LOGGER.log(System.Logger.Level.WARNING, "standby bootstrap failed; will retry: " + ex);
            }
            else
            {
                fence("standby bootstrap failed after start: " + ex);
            }
        }
    }

    private void startReplay()
    {
        replaySessionId = (int)remoteArchive.startReplay(
            followRecordingId, followPosition, AeronArchive.NULL_LENGTH, replicationChannel,
            HaConfig.REPLICATION_STREAM_ID);
    }

    private void reconnectReplication()
    {
        try
        {
            CloseHelper.quietClose(remoteArchive);
            remoteArchive = null;
            remoteArchive = EngineArchive.connectRemote(aeron, ha.peerHost(), ha.peerArchivePort(), ha.selfHost());
            startReplay();
            followReconnects.incrementAndGet();
            LOGGER.log(System.Logger.Level.INFO, "replication resumed at journal position " + followPosition);
        }
        catch (final RuntimeException ex)
        {
            LOGGER.log(System.Logger.Level.WARNING, "replication reconnect failed: " + ex);
        }
    }

    private void onReplicatedEntry(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (header.sessionId() != replaySessionId || header.position() <= followPosition)
        {
            return;
        }
        core.onJournalEntry(buffer, offset, length, false);
        followPosition = header.position();
    }

    private void drainReplication()
    {
        if (replaySubscription == null)
        {
            return;
        }
        int idlePolls = 0;
        while (idlePolls < 3)
        {
            if (replaySubscription.poll(replicationHandler, Integer.MAX_VALUE) == 0)
            {
                idlePolls++;
            }
            else
            {
                idlePolls = 0;
            }
        }
    }

    private void closeFollower()
    {
        CloseHelper.quietCloseAll(replaySubscription, remoteArchive);
        replaySubscription = null;
        remoteArchive = null;
        peerVisible = false;
    }

    private void fence(final String reason)
    {
        LOGGER.log(System.Logger.Level.WARNING, "node " + ha.nodeId() + " FENCED: " + reason);
        link.fence();
        final EngineCore current = core;
        if (current != null)
        {
            current.ingress().publishRoleChange(NodeRole.FENCED, epoch);
        }
        role = NodeRole.FENCED;
        closeFollower();
        onFenced.run();
    }

    @Override
    public void onClose()
    {
        closeFollower();
        if (link != null)
        {
            link.close();
        }
        CloseHelper.quietCloseAll(heartbeatPublication, arbiterSubscription, ackSubscription, ackPublication);
    }

    /**
     * Core (null until the node has a role).
     *
     * @return core.
     */
    public EngineCore core()
    {
        return core;
    }

    /**
     * Current role.
     *
     * @return role.
     */
    public NodeRole role()
    {
        return role;
    }

    /**
     * Current epoch.
     *
     * @return epoch.
     */
    public long epoch()
    {
        return epoch;
    }

    /**
     * Replication link (HA only).
     *
     * @return link or {@code null}.
     */
    public ReplicationLink link()
    {
        return link;
    }

    /**
     * Whether the node was fenced.
     *
     * @return true once fenced.
     */
    public boolean isFenced()
    {
        return role == NodeRole.FENCED;
    }

    /**
     * Monotonic time of the last promotion.
     *
     * @return nanoseconds, 0 if never promoted.
     */
    public long lastPromotionNs()
    {
        return lastPromotionNs;
    }

    /**
     * HA and durability status for the {@code /status} endpoint.
     *
     * @return status entries.
     */
    public Map<String, Object> status()
    {
        final Map<String, Object> status = new LinkedHashMap<>();
        status.put("role", role.name());
        status.put("epoch", epoch);
        final EngineCore current = core;
        if (current != null)
        {
            status.put("lastSequence", current.sequencer().lastSequence());
            status.put("lastReleasedSequence", current.pipeline().publisher().lastReleasedEngineSequence());
            status.put("retainedMessages", current.pipeline().publisher().retainedMessages());
            status.put("duplicatesRejected", current.ingress().duplicatesDetected());
            if (current.journal() != null)
            {
                status.put("journalRecordingId", current.journal().recordingId());
                status.put("journalPosition", current.journal().position());
                status.put("journalRecordedPosition", current.journal().recordedPosition());
                status.put("snapshotsWritten", current.snapshotStore().snapshotsWritten());
                status.put("lastSnapshotSequence", current.snapshotStore().lastSnapshotSequence());
            }
        }
        if (link != null)
        {
            status.put("releaseMode", link.mode().name());
            status.put("leaseValid", link.leaseValid());
            status.put("standbyAttached", link.standbyAttached());
            status.put("degraded", link.isDegraded());
            status.put("ackedSequence", link.ackedSequence());
            status.put("followPosition", followPosition);
            status.put("peerVisible", peerVisible);
            status.put("promotions", promotions.get());
            status.put("replicationReconnects", followReconnects.get());
        }
        return status;
    }
}
