package com.flashexchange.engine.pipeline;

import com.flashexchange.protocol.JournalEntryFlyweight;
import com.flashexchange.protocol.NodeRole;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Pre-allocated Disruptor ring slot. The ingress thread copies a decoded command into the slot and stamps it with
 * the engine sequence and timestamp; the owning matcher shard processes it and encodes its output into
 * {@link #executionOutput()} and {@link #marketDataOutput()}; the publisher sends the output and clears the slot.
 */
public final class OrderCommand
{
    CommandType type;
    long engineSequence;
    long timestamp;
    long orderId;
    long clientOrderId;
    long userId;
    long price;
    long quantity;
    int instrumentId;
    byte side;
    byte orderType;
    byte timeInForce;
    byte selfTradePrevention;
    boolean postOnly;
    boolean duplicate;
    boolean replay;
    long epoch;
    long journalPosition;
    long releasedSequence = -1;
    NodeRole role;

    private ExpandableDirectByteBuffer[] sections = new ExpandableDirectByteBuffer[0];
    private int[] sectionLengths = new int[0];
    private final MessageBatch executionOutput = new MessageBatch(1024);
    private final MessageBatch marketDataOutput = new MessageBatch(1024);

    /**
     * Populate the slot with a new order command.
     *
     * @param clientOrderId       client order id.
     * @param userId              user.
     * @param instrumentId        instrument.
     * @param side                side wire code.
     * @param orderType           order type wire code.
     * @param timeInForce         time in force wire code.
     * @param selfTradePrevention self-trade prevention wire code.
     * @param postOnly            post-only flag.
     * @param price               limit price.
     * @param quantity            quantity.
     * @return this for fluent use.
     */
    public OrderCommand newOrder(
        final long clientOrderId,
        final long userId,
        final int instrumentId,
        final byte side,
        final byte orderType,
        final byte timeInForce,
        final byte selfTradePrevention,
        final boolean postOnly,
        final long price,
        final long quantity)
    {
        reset(CommandType.NEW_ORDER);
        this.clientOrderId = clientOrderId;
        this.userId = userId;
        this.instrumentId = instrumentId;
        this.side = side;
        this.orderType = orderType;
        this.timeInForce = timeInForce;
        this.selfTradePrevention = selfTradePrevention;
        this.postOnly = postOnly;
        this.price = price;
        this.quantity = quantity;
        this.orderId = 0;
        return this;
    }

    /**
     * Populate the slot with a cancel command.
     *
     * @param clientOrderId client id of the cancel request.
     * @param userId        requesting user.
     * @param instrumentId  instrument.
     * @param orderId       order to cancel.
     * @return this for fluent use.
     */
    public OrderCommand cancelOrder(
        final long clientOrderId, final long userId, final int instrumentId, final long orderId)
    {
        reset(CommandType.CANCEL_ORDER);
        this.clientOrderId = clientOrderId;
        this.userId = userId;
        this.instrumentId = instrumentId;
        this.orderId = orderId;
        return this;
    }

    /**
     * Populate the slot with a snapshot command for one instrument.
     *
     * @param instrumentId instrument.
     * @return this for fluent use.
     */
    public OrderCommand bookSnapshot(final int instrumentId)
    {
        reset(CommandType.BOOK_SNAPSHOT);
        this.instrumentId = instrumentId;
        return this;
    }

    /**
     * Populate the slot with a state snapshot command. The producer writes the sequencer state into section 0 and
     * every matcher shard writes its books into section {@code shard + 1}.
     *
     * @param sectionCount number of sections (shards + 1).
     * @return this for fluent use.
     */
    public OrderCommand stateSnapshot(final int sectionCount)
    {
        reset(CommandType.STATE_SNAPSHOT);
        if (sections.length != sectionCount)
        {
            sections = new ExpandableDirectByteBuffer[sectionCount];
            sectionLengths = new int[sectionCount];
        }
        for (int i = 0; i < sectionCount; i++)
        {
            if (sections[i] == null)
            {
                sections[i] = new ExpandableDirectByteBuffer(4096);
            }
            sectionLengths[i] = 0;
        }
        return this;
    }

    /**
     * Populate the slot with a role change announcement.
     *
     * @param role  new role of this node.
     * @param epoch new epoch.
     * @return this for fluent use.
     */
    public OrderCommand roleChange(final NodeRole role, final long epoch)
    {
        reset(CommandType.ROLE_CHANGE);
        this.role = role;
        this.epoch = epoch;
        return this;
    }

    /**
     * Populate the slot from a journal entry (recovery replay or replication). The original sequence, timestamp,
     * order id, epoch and duplicate verdict are kept, which makes processing deterministic.
     *
     * @param entry  decoded journal entry.
     * @param replay true if the entry is already in the local journal and must not be journaled again.
     * @return this for fluent use.
     */
    public OrderCommand fromJournal(final JournalEntryFlyweight entry, final boolean replay)
    {
        reset(CommandType.journaled(entry.commandType()));
        this.engineSequence = entry.engineSequence();
        this.timestamp = entry.timestamp();
        this.orderId = entry.orderId();
        this.clientOrderId = entry.clientOrderId();
        this.userId = entry.userId();
        this.price = entry.price();
        this.quantity = entry.quantity();
        this.epoch = entry.epoch();
        this.instrumentId = entry.instrumentId();
        this.side = entry.side();
        this.orderType = entry.orderType();
        this.timeInForce = entry.timeInForce();
        this.selfTradePrevention = entry.selfTradePrevention();
        this.postOnly = entry.isPostOnly();
        this.duplicate = entry.isDuplicate();
        this.releasedSequence = entry.releasedSequence();
        this.replay = replay;
        return this;
    }

    /**
     * Encode the slot as a journal entry.
     *
     * @param entry    flyweight wrapped for encoding.
     * @param released highest sequence released to clients by the writer.
     */
    public void toJournal(final JournalEntryFlyweight entry, final long released)
    {
        byte flags = 0;
        if (postOnly)
        {
            flags |= JournalEntryFlyweight.FLAG_POST_ONLY;
        }
        if (duplicate)
        {
            flags |= JournalEntryFlyweight.FLAG_DUPLICATE;
        }
        entry.engineSequence(engineSequence)
            .timestamp(timestamp)
            .orderId(orderId)
            .clientOrderId(clientOrderId)
            .userId(userId)
            .price(price)
            .quantity(quantity)
            .epoch(epoch)
            .instrumentId(instrumentId)
            .commandType(type.code())
            .side(side)
            .orderType(orderType)
            .timeInForce(timeInForce)
            .selfTradePrevention(selfTradePrevention)
            .flags(flags)
            .releasedSequence(released);
    }

    private void reset(final CommandType newType)
    {
        this.type = newType;
        this.duplicate = false;
        this.replay = false;
        this.journalPosition = 0;
        this.releasedSequence = -1;
        this.role = null;
        this.orderId = 0;
    }

    /**
     * Flag a new order as a duplicate client order id (it will be rejected).
     *
     * @return this for fluent use.
     */
    public OrderCommand markDuplicate()
    {
        this.duplicate = true;
        return this;
    }

    /**
     * Set the epoch the command is sequenced in.
     *
     * @param value epoch.
     * @return this for fluent use.
     */
    public OrderCommand epoch(final long value)
    {
        this.epoch = value;
        return this;
    }

    /**
     * Epoch the command was sequenced in (or the new epoch of a role change).
     *
     * @return epoch.
     */
    public long epoch()
    {
        return epoch;
    }

    /**
     * Whether the sequencer flagged the order as a duplicate.
     *
     * @return duplicate flag.
     */
    public boolean isDuplicate()
    {
        return duplicate;
    }

    /**
     * Whether the command is a replay of the local journal (not journaled again).
     *
     * @return replay flag.
     */
    public boolean isReplay()
    {
        return replay;
    }

    /**
     * Journal publication position after this command was journaled (0 if not journaled).
     *
     * @return position.
     */
    public long journalPosition()
    {
        return journalPosition;
    }

    /**
     * Set the journal position (journal stage).
     *
     * @param value position.
     */
    public void journalPosition(final long value)
    {
        this.journalPosition = value;
    }

    /**
     * Released sequence carried by a replicated entry (-1 if unknown).
     *
     * @return sequence.
     */
    public long releasedSequence()
    {
        return releasedSequence;
    }

    /**
     * New role of a role change command.
     *
     * @return role or {@code null}.
     */
    public NodeRole role()
    {
        return role;
    }

    /**
     * Number of snapshot sections of a state snapshot command.
     *
     * @return count.
     */
    public int sectionCount()
    {
        return type == CommandType.STATE_SNAPSHOT ? sectionLengths.length : 0;
    }

    /**
     * Buffer of a snapshot section.
     *
     * @param index section.
     * @return buffer.
     */
    public MutableDirectBuffer section(final int index)
    {
        return sections[index];
    }

    /**
     * Length of the data written into a snapshot section.
     *
     * @param index section.
     * @return length.
     */
    public int sectionLength(final int index)
    {
        return sectionLengths[index];
    }

    /**
     * Record the length of a snapshot section.
     *
     * @param index  section.
     * @param length bytes written.
     */
    public void sectionLength(final int index, final int length)
    {
        sectionLengths[index] = length;
    }

    /**
     * Client order id.
     *
     * @return client order id.
     */
    public long clientOrderId()
    {
        return clientOrderId;
    }

    /**
     * User.
     *
     * @return user id.
     */
    public long userId()
    {
        return userId;
    }

    /**
     * Stamp the slot with the engine sequence and timestamp. New orders receive {@code orderId = sequence + 1}, which
     * makes order ids unique, monotonic and deterministic.
     *
     * @param engineSequence engine sequence.
     * @param timestamp      engine timestamp in epoch nanoseconds.
     * @return this for fluent use.
     */
    public OrderCommand stamp(final long engineSequence, final long timestamp)
    {
        this.engineSequence = engineSequence;
        this.timestamp = timestamp;
        if (type == CommandType.NEW_ORDER)
        {
            this.orderId = engineSequence + 1;
        }
        return this;
    }

    /**
     * Command type.
     *
     * @return type.
     */
    public CommandType type()
    {
        return type;
    }

    /**
     * Engine sequence.
     *
     * @return sequence.
     */
    public long engineSequence()
    {
        return engineSequence;
    }

    /**
     * Engine timestamp.
     *
     * @return epoch nanoseconds.
     */
    public long timestamp()
    {
        return timestamp;
    }

    /**
     * Order id (assigned for new orders, target for cancels).
     *
     * @return order id.
     */
    public long orderId()
    {
        return orderId;
    }

    /**
     * Instrument.
     *
     * @return instrument id.
     */
    public int instrumentId()
    {
        return instrumentId;
    }

    /**
     * Encoded private events produced by the command.
     *
     * @return execution output.
     */
    public MessageBatch executionOutput()
    {
        return executionOutput;
    }

    /**
     * Encoded market data produced by the command.
     *
     * @return market data output.
     */
    public MessageBatch marketDataOutput()
    {
        return marketDataOutput;
    }

    /**
     * Clear output after publication.
     */
    public void clearOutput()
    {
        executionOutput.clear();
        marketDataOutput.clear();
    }
}
