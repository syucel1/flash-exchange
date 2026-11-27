package com.flashexchange.engine.ha;

import com.flashexchange.engine.config.ReleaseMode;
import com.flashexchange.engine.durability.ReplicationGate;
import com.flashexchange.protocol.MessageHeader;
import com.flashexchange.protocol.ReplicationAckFlyweight;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Replication half of the release gate between a primary and its standby (stream 102 carries the acks).
 * <p>
 * Primary: {@link #awaitAck(long)} holds a command's output until the standby acknowledged its sequence. With no
 * standby attached, in {@link ReleaseMode#ASYNC}, or once an ack timed out (the link is then <em>degraded</em> until
 * the standby catches up) output is released without the ack, but only while the node holds a valid lease from the
 * arbiter. Without a lease output is withheld; after fencing it is dropped. This is what prevents a primary that lost
 * the arbiter from publishing a history the promoted standby never saw.
 * <p>
 * Standby: {@link #sendAck(long)} acknowledges sequences durable in its own journal.
 * <p>
 * {@link #awaitAck(long)} and {@link #sendAck(long)} run on the publisher thread, {@link #pollAcks(long)} and the
 * lease and fence updates on the conductor thread.
 */
public final class ReplicationLink implements ReplicationGate
{
    private final ReleaseMode mode;
    private final long ackTimeoutNs;
    private final int nodeId;
    private final Subscription ackSubscription;
    private final Publication ackPublication;
    private final LongSupplier nanoClock;
    private final IdleStrategy idle = new BackoffIdleStrategy();
    private final UnsafeBuffer sendBuffer =
        new UnsafeBuffer(ByteBuffer.allocateDirect(ReplicationAckFlyweight.ENCODED_LENGTH));
    private final ReplicationAckFlyweight sendAck = new ReplicationAckFlyweight();
    private final UnsafeBuffer keepAliveBuffer =
        new UnsafeBuffer(ByteBuffer.allocateDirect(ReplicationAckFlyweight.ENCODED_LENGTH));
    private final ReplicationAckFlyweight keepAliveAck = new ReplicationAckFlyweight();
    private final ReplicationAckFlyweight receivedAck = new ReplicationAckFlyweight();
    private final FragmentHandler ackHandler = this::onAck;

    private volatile long ackedSequence = -1;
    private volatile long leaseExpiryNs;
    private volatile boolean leaseGranted;
    private volatile boolean leaseRequired = true;
    private volatile boolean fenced;
    private volatile boolean degraded;
    private volatile boolean closed;
    private volatile boolean ackSeen;
    private volatile long epoch;
    private long acksReceived;
    private long degradations;

    /**
     * Create the link.
     *
     * @param mode            SYNC (wait for acks) or ASYNC.
     * @param ackTimeoutMs    how long to wait for an ack before degrading to ASYNC.
     * @param nodeId          this node.
     * @param ackSubscription acks from the standby (used as primary), may be {@code null}.
     * @param ackPublication  acks to the primary (used as standby), may be {@code null}.
     * @param nanoClock       monotonic clock in nanoseconds.
     */
    public ReplicationLink(
        final ReleaseMode mode,
        final long ackTimeoutMs,
        final int nodeId,
        final Subscription ackSubscription,
        final Publication ackPublication,
        final LongSupplier nanoClock)
    {
        this.mode = mode;
        this.ackTimeoutNs = TimeUnit.MILLISECONDS.toNanos(ackTimeoutMs);
        this.nodeId = nodeId;
        this.ackSubscription = ackSubscription;
        this.ackPublication = ackPublication;
        this.nanoClock = nanoClock;
    }

    @Override
    public boolean awaitAck(final long engineSequence)
    {
        if (mode == ReleaseMode.ASYNC || degraded || !standbyAttached())
        {
            return awaitLease();
        }
        final long startNs = nanoClock.getAsLong();
        idle.reset();
        while (ackedSequence < engineSequence)
        {
            checkState();
            if (fenced)
            {
                return false;
            }
            if (degraded || nanoClock.getAsLong() - startNs > ackTimeoutNs)
            {
                if (!degraded)
                {
                    degraded = true;
                    degradations++;
                }
                return awaitLease();
            }
            idle.idle();
        }
        return !fenced;
    }

    private boolean awaitLease()
    {
        idle.reset();
        while (!leaseValid())
        {
            checkState();
            if (fenced)
            {
                return false;
            }
            idle.idle();
        }
        return !fenced;
    }

    private void checkState()
    {
        if (closed)
        {
            throw new IllegalStateException("engine closing: output withheld");
        }
    }

    @Override
    public void sendAck(final long engineSequence)
    {
        if (ackPublication == null)
        {
            return;
        }
        sendAck.wrapForEncode(sendBuffer, 0).ackedSequence(engineSequence).epoch(epoch).nodeId(nodeId);
        ackPublication.offer(sendBuffer, 0, ReplicationAckFlyweight.ENCODED_LENGTH);
    }

    /**
     * Standby, conductor thread: re-send the highest durable sequence so an idle primary knows a standby is
     * attached (the ack publication is thread safe; this uses its own buffer).
     *
     * @param engineSequence highest durable sequence.
     */
    public void sendKeepAlive(final long engineSequence)
    {
        if (ackPublication == null)
        {
            return;
        }
        keepAliveAck.wrapForEncode(keepAliveBuffer, 0).ackedSequence(engineSequence).epoch(epoch).nodeId(nodeId);
        ackPublication.offer(keepAliveBuffer, 0, ReplicationAckFlyweight.ENCODED_LENGTH);
    }

    /**
     * Poll acknowledgements from the standby (conductor thread, primary role).
     *
     * @param lastJournaledSequence highest sequence journaled locally, used to leave degraded mode once the
     *                              standby has caught up.
     * @return fragments read.
     */
    public int pollAcks(final long lastJournaledSequence)
    {
        if (ackSubscription == null)
        {
            return 0;
        }
        final int fragments = ackSubscription.poll(ackHandler, 16);
        if (degraded && ackSeen && ackedSequence >= lastJournaledSequence)
        {
            degraded = false;
        }
        return fragments;
    }

    private void onAck(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (length < ReplicationAckFlyweight.ENCODED_LENGTH ||
            MessageHeader.templateId(buffer, offset) != ReplicationAckFlyweight.TEMPLATE_ID)
        {
            return;
        }
        receivedAck.wrapForDecode(buffer, offset);
        acksReceived++;
        ackSeen = true;
        if (receivedAck.ackedSequence() > ackedSequence)
        {
            ackedSequence = receivedAck.ackedSequence();
        }
    }

    /**
     * Whether a standby is attached and acknowledging: its ack stream is connected and has delivered an ack since
     * the last time an image (re)appeared.
     *
     * @return true when SYNC release waits for acks.
     */
    public boolean standbyAttached()
    {
        if (ackSubscription == null || !ackSubscription.isConnected())
        {
            ackSeen = false;
            return false;
        }
        return ackSeen;
    }

    /**
     * Called when the ack image changes (a standby attached or left).
     *
     * @param image the image.
     */
    public void onAckImageChange(final Image image)
    {
        ackSeen = false;
    }

    /**
     * Grant (or extend) the lease until a point in time.
     *
     * @param expiryNs monotonic expiry.
     */
    public void leaseUntil(final long expiryNs)
    {
        leaseExpiryNs = expiryNs;
        leaseGranted = true;
    }

    /**
     * Standalone and standby operation does not need a lease.
     *
     * @param required whether output release requires a lease.
     */
    public void leaseRequired(final boolean required)
    {
        leaseRequired = required;
    }

    /**
     * Whether the node may release output and accept commands.
     *
     * @return true when no lease is required or the lease has not expired.
     */
    public boolean leaseValid()
    {
        return !leaseRequired || (leaseGranted && nanoClock.getAsLong() - leaseExpiryNs < 0);
    }

    /**
     * Fence the node: withheld output is dropped and nothing more is released.
     */
    public void fence()
    {
        fenced = true;
    }

    /**
     * Whether the node has been fenced.
     *
     * @return true after {@link #fence()}.
     */
    public boolean isFenced()
    {
        return fenced;
    }

    /**
     * Current epoch (stamped on acks).
     *
     * @param value epoch.
     */
    public void epoch(final long value)
    {
        epoch = value;
    }

    /**
     * Highest sequence acknowledged by the standby.
     *
     * @return sequence, -1 before the first ack.
     */
    public long ackedSequence()
    {
        return ackedSequence;
    }

    /**
     * Whether SYNC release has degraded to ASYNC after an ack timeout.
     *
     * @return true while degraded.
     */
    public boolean isDegraded()
    {
        return degraded;
    }

    /**
     * Configured release mode.
     *
     * @return mode.
     */
    public ReleaseMode mode()
    {
        return mode;
    }

    /**
     * Acks received (conductor thread counter, approximate when read elsewhere).
     *
     * @return count.
     */
    public long acksReceived()
    {
        return acksReceived;
    }

    /**
     * Times the link degraded to ASYNC.
     *
     * @return count.
     */
    public long degradations()
    {
        return degradations;
    }

    /**
     * Unblock any waiting publisher; the engine is shutting down.
     */
    public void close()
    {
        closed = true;
    }
}
