package com.flashexchange.engine.transport;

import com.flashexchange.engine.pipeline.OrderCommand;
import com.flashexchange.protocol.BookSnapshotRequestFlyweight;
import com.flashexchange.protocol.CancelOrderFlyweight;
import com.flashexchange.protocol.MessageHeader;
import com.flashexchange.protocol.NewOrderFlyweight;
import com.lmax.disruptor.RingBuffer;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochNanoClock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Ingress agent: polls the Aeron command subscription, decodes each command and publishes it into the Disruptor
 * ring, stamping the engine sequence and timestamp. It is the ring's single producer.
 * <p>
 * When the ring is full {@code RingBuffer#next()} spins, which stops polling and lets Aeron flow control push back on
 * the senders: commands are never dropped by the engine. Malformed or unknown messages are counted and skipped.
 * The agent also injects periodic snapshot commands for every instrument.
 */
public final class CommandIngress implements Agent
{
    private final Subscription subscription;
    private final RingBuffer<OrderCommand> ringBuffer;
    private final EpochNanoClock clock;
    private final int fragmentLimit;
    private final int[] instrumentIds;
    private final long snapshotIntervalNs;
    private final FragmentHandler fragmentHandler;

    private final NewOrderFlyweight newOrder = new NewOrderFlyweight();
    private final CancelOrderFlyweight cancelOrder = new CancelOrderFlyweight();
    private final BookSnapshotRequestFlyweight snapshotRequest = new BookSnapshotRequestFlyweight();

    private final AtomicLong commandsReceived = new AtomicLong();
    private final AtomicLong malformedMessages = new AtomicLong();
    private final AtomicLong snapshotsScheduled = new AtomicLong();
    private long nextSnapshotNs;

    /**
     * Create the ingress.
     *
     * @param subscription       command subscription (may be {@code null} when driven directly via
     *                           {@link #onMessage(DirectBuffer, int, int)}).
     * @param ringBuffer         ring to publish into.
     * @param clock              engine clock (epoch nanoseconds).
     * @param fragmentLimit      maximum fragments per poll.
     * @param instrumentIds      every configured instrument (used for "all instruments" snapshots).
     * @param snapshotIntervalNs period of automatic snapshots (0 = disabled).
     */
    public CommandIngress(
        final Subscription subscription,
        final RingBuffer<OrderCommand> ringBuffer,
        final EpochNanoClock clock,
        final int fragmentLimit,
        final int[] instrumentIds,
        final long snapshotIntervalNs)
    {
        this.subscription = subscription;
        this.ringBuffer = ringBuffer;
        this.clock = clock;
        this.fragmentLimit = fragmentLimit;
        this.instrumentIds = instrumentIds.clone();
        this.snapshotIntervalNs = snapshotIntervalNs;
        this.fragmentHandler = new FragmentAssembler((buffer, offset, length, header) ->
            onMessage(buffer, offset, length));
        this.nextSnapshotNs = snapshotIntervalNs > 0 ? clock.nanoTime() + snapshotIntervalNs : Long.MAX_VALUE;
    }

    @Override
    public int doWork()
    {
        int workCount = 0;
        if (subscription != null)
        {
            workCount += subscription.poll(fragmentHandler, fragmentLimit);
        }
        workCount += checkSnapshotTimer();
        return workCount;
    }

    @Override
    public String roleName()
    {
        return "engine-ingress";
    }

    /**
     * Decode one command message and publish it into the ring.
     *
     * @param buffer containing the message.
     * @param offset of the message header.
     * @param length of the message.
     */
    public void onMessage(final DirectBuffer buffer, final int offset, final int length)
    {
        if (length < MessageHeader.LENGTH || MessageHeader.schemaId(buffer, offset) != MessageHeader.SCHEMA_ID)
        {
            malformedMessages.incrementAndGet();
            return;
        }

        switch (MessageHeader.templateId(buffer, offset))
        {
            case NewOrderFlyweight.TEMPLATE_ID:
                if (MessageHeader.isValid(buffer, offset, length, NewOrderFlyweight.BLOCK_LENGTH))
                {
                    newOrder.wrapForDecode(buffer, offset);
                    final long sequence = ringBuffer.next();
                    try
                    {
                        ringBuffer.get(sequence).newOrder(
                            newOrder.clientOrderId(),
                            newOrder.userId(),
                            newOrder.instrumentId(),
                            newOrder.sideCode(),
                            newOrder.orderTypeCode(),
                            newOrder.timeInForceCode(),
                            newOrder.selfTradePreventionCode(),
                            newOrder.isPostOnly(),
                            newOrder.price(),
                            newOrder.quantity()).stamp(sequence, clock.nanoTime());
                    }
                    finally
                    {
                        ringBuffer.publish(sequence);
                    }
                    commandsReceived.incrementAndGet();
                    return;
                }
                break;

            case CancelOrderFlyweight.TEMPLATE_ID:
                if (MessageHeader.isValid(buffer, offset, length, CancelOrderFlyweight.BLOCK_LENGTH))
                {
                    cancelOrder.wrapForDecode(buffer, offset);
                    final long sequence = ringBuffer.next();
                    try
                    {
                        ringBuffer.get(sequence).cancelOrder(
                            cancelOrder.clientOrderId(),
                            cancelOrder.userId(),
                            cancelOrder.instrumentId(),
                            cancelOrder.orderId()).stamp(sequence, clock.nanoTime());
                    }
                    finally
                    {
                        ringBuffer.publish(sequence);
                    }
                    commandsReceived.incrementAndGet();
                    return;
                }
                break;

            case BookSnapshotRequestFlyweight.TEMPLATE_ID:
                if (MessageHeader.isValid(buffer, offset, length, BookSnapshotRequestFlyweight.BLOCK_LENGTH))
                {
                    snapshotRequest.wrapForDecode(buffer, offset);
                    final int instrumentId = snapshotRequest.instrumentId();
                    if (instrumentId == BookSnapshotRequestFlyweight.ALL_INSTRUMENTS)
                    {
                        publishSnapshotsForAll();
                    }
                    else
                    {
                        publishSnapshot(instrumentId);
                    }
                    commandsReceived.incrementAndGet();
                    return;
                }
                break;

            default:
                break;
        }

        malformedMessages.incrementAndGet();
    }

    private int checkSnapshotTimer()
    {
        if (snapshotIntervalNs <= 0)
        {
            return 0;
        }
        final long nowNs = clock.nanoTime();
        if (nowNs - nextSnapshotNs < 0)
        {
            return 0;
        }
        nextSnapshotNs = nowNs + snapshotIntervalNs;
        publishSnapshotsForAll();
        return 1;
    }

    private void publishSnapshotsForAll()
    {
        for (final int instrumentId : instrumentIds)
        {
            publishSnapshot(instrumentId);
        }
    }

    private void publishSnapshot(final int instrumentId)
    {
        final long sequence = ringBuffer.next();
        try
        {
            ringBuffer.get(sequence).bookSnapshot(instrumentId).stamp(sequence, clock.nanoTime());
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
        snapshotsScheduled.incrementAndGet();
    }

    /**
     * Valid commands received.
     *
     * @return count.
     */
    public long commandsReceived()
    {
        return commandsReceived.get();
    }

    /**
     * Messages skipped because they were malformed or of an unknown type.
     *
     * @return count.
     */
    public long malformedMessages()
    {
        return malformedMessages.get();
    }

    /**
     * Snapshot commands injected into the ring (requested or periodic).
     *
     * @return count.
     */
    public long snapshotsScheduled()
    {
        return snapshotsScheduled.get();
    }
}
