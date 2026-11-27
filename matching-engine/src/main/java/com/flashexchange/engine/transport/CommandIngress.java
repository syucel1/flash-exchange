package com.flashexchange.engine.transport;

import com.flashexchange.engine.ha.Sequencer;
import com.flashexchange.engine.pipeline.CommandType;
import com.flashexchange.engine.pipeline.OrderCommand;
import com.flashexchange.protocol.JournalEntryFlyweight;
import com.flashexchange.protocol.NodeRole;
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
 * ring, stamping the engine sequence (from the {@link Sequencer}), timestamp and epoch. It is the ring's single
 * producer, so the conductor also uses it to inject journal entries (recovery and replication), state snapshots
 * and role changes. Only new orders and cancels consume an engine sequence; book snapshots, state snapshots and
 * role changes carry the last assigned sequence, so the sequence stream is identical when the journal is replayed.
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
    private final FragmentHandler discardHandler;
    private final Sequencer sequencer;
    private long epoch;

    private final NewOrderFlyweight newOrder = new NewOrderFlyweight();
    private final CancelOrderFlyweight cancelOrder = new CancelOrderFlyweight();
    private final BookSnapshotRequestFlyweight snapshotRequest = new BookSnapshotRequestFlyweight();

    private final AtomicLong commandsReceived = new AtomicLong();
    private final AtomicLong malformedMessages = new AtomicLong();
    private final AtomicLong snapshotsScheduled = new AtomicLong();
    private final AtomicLong commandsDiscarded = new AtomicLong();
    private final AtomicLong duplicatesDetected = new AtomicLong();
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
        this(subscription, ringBuffer, clock, fragmentLimit, instrumentIds, snapshotIntervalNs, new Sequencer(0));
    }

    /**
     * Create the ingress with an explicit sequencer.
     *
     * @param subscription       command subscription (may be {@code null}).
     * @param ringBuffer         ring to publish into.
     * @param clock              engine clock (epoch nanoseconds).
     * @param fragmentLimit      maximum fragments per poll.
     * @param instrumentIds      every configured instrument.
     * @param snapshotIntervalNs period of automatic book snapshots (0 = disabled).
     * @param sequencer          assigns engine sequences and detects duplicates.
     */
    public CommandIngress(
        final Subscription subscription,
        final RingBuffer<OrderCommand> ringBuffer,
        final EpochNanoClock clock,
        final int fragmentLimit,
        final int[] instrumentIds,
        final long snapshotIntervalNs,
        final Sequencer sequencer)
    {
        this.sequencer = sequencer;
        this.discardHandler = (buffer, offset, length, header) -> commandsDiscarded.incrementAndGet();
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
        return doWork(fragmentLimit);
    }

    /**
     * Poll at most a given number of command fragments and run the snapshot timer.
     *
     * @param limit maximum fragments to poll.
     * @return work done.
     */
    public int doWork(final int limit)
    {
        int workCount = 0;
        if (subscription != null)
        {
            workCount += subscription.poll(fragmentHandler, Math.min(limit, fragmentLimit));
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
                    final long engineSequence = sequencer.next();
                    final boolean duplicate = sequencer.checkDuplicate(newOrder.userId(), newOrder.clientOrderId());
                    final long sequence = ringBuffer.next();
                    try
                    {
                        final OrderCommand slot = ringBuffer.get(sequence);
                        slot.newOrder(
                            newOrder.clientOrderId(),
                            newOrder.userId(),
                            newOrder.instrumentId(),
                            newOrder.sideCode(),
                            newOrder.orderTypeCode(),
                            newOrder.timeInForceCode(),
                            newOrder.selfTradePreventionCode(),
                            newOrder.isPostOnly(),
                            newOrder.price(),
                            newOrder.quantity()).stamp(engineSequence, clock.nanoTime()).epoch(epoch);
                        if (duplicate)
                        {
                            slot.markDuplicate();
                            duplicatesDetected.incrementAndGet();
                        }
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
                    final long engineSequence = sequencer.next();
                    final long sequence = ringBuffer.next();
                    try
                    {
                        ringBuffer.get(sequence).cancelOrder(
                            cancelOrder.clientOrderId(),
                            cancelOrder.userId(),
                            cancelOrder.instrumentId(),
                            cancelOrder.orderId()).stamp(engineSequence, clock.nanoTime()).epoch(epoch);
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
            ringBuffer.get(sequence).bookSnapshot(instrumentId)
                .stamp(sequencer.lastSequence(), clock.nanoTime()).epoch(epoch);
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
        snapshotsScheduled.incrementAndGet();
    }

    /**
     * Poll the command subscription and drop everything (standby and pending nodes: the primary owns the commands).
     *
     * @return fragments discarded.
     */
    public int discard()
    {
        return subscription == null ? 0 : subscription.poll(discardHandler, fragmentLimit);
    }

    /**
     * Publish a command sequenced elsewhere (local journal replay or replication from the primary).
     *
     * @param entry  decoded journal entry.
     * @param replay true if the entry already is in the local journal.
     */
    public void publishJournalEntry(final JournalEntryFlyweight entry, final boolean replay)
    {
        final long sequence = ringBuffer.next();
        try
        {
            final OrderCommand slot = ringBuffer.get(sequence).fromJournal(entry, replay);
            sequencer.observe(slot.engineSequence(), slot.type() == CommandType.NEW_ORDER, slot.isDuplicate(),
                slot.userId(), slot.clientOrderId());
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Publish a state snapshot command; the sequencer state is written into section 0.
     *
     * @param shardCount number of matcher shards.
     */
    public void publishStateSnapshot(final int shardCount)
    {
        final long sequence = ringBuffer.next();
        try
        {
            final OrderCommand slot = ringBuffer.get(sequence).stateSnapshot(shardCount + 1);
            slot.stamp(sequencer.lastSequence(), clock.nanoTime()).epoch(epoch);
            slot.sectionLength(0, sequencer.write(slot.section(0), 0));
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Publish a role change so the publisher announces it in sequence with the output stream.
     *
     * @param role     new role.
     * @param newEpoch new epoch (also used for subsequently sequenced commands).
     */
    public void publishRoleChange(final NodeRole role, final long newEpoch)
    {
        this.epoch = newEpoch;
        final long sequence = ringBuffer.next();
        try
        {
            ringBuffer.get(sequence).roleChange(role, newEpoch).stamp(sequencer.lastSequence(), clock.nanoTime());
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Sequencer.
     *
     * @return sequencer.
     */
    public Sequencer sequencer()
    {
        return sequencer;
    }

    /**
     * Epoch stamped on new commands.
     *
     * @return epoch.
     */
    public long epoch()
    {
        return epoch;
    }

    /**
     * Commands dropped because this node does not own the command stream.
     *
     * @return count.
     */
    public long commandsDiscarded()
    {
        return commandsDiscarded.get();
    }

    /**
     * New orders flagged as duplicate client order ids.
     *
     * @return count.
     */
    public long duplicatesDetected()
    {
        return duplicatesDetected.get();
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
