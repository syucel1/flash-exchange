package com.flashexchange.engine.ha;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

/**
 * Assigns engine sequences and detects duplicate client order ids. Owned by the single thread that produces into
 * the pipeline (the conductor). Engine sequences are global, start at 0, never repeat and survive restarts and
 * failovers through snapshots and the journal.
 */
public final class Sequencer
{
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    private static final int STATE_VERSION = 1;

    private final DedupWindow dedupWindow;
    private final boolean dedupEnabled;
    private long lastSequence = -1;

    /**
     * Create a sequencer.
     *
     * @param dedupCapacity number of recent (user, client order id) pairs checked for duplicates (0 disables
     *                      duplicate detection). Client order id 0 is never checked.
     */
    public Sequencer(final int dedupCapacity)
    {
        this.dedupEnabled = dedupCapacity > 0;
        this.dedupWindow = new DedupWindow(Math.max(1, dedupCapacity));
    }

    /**
     * Assign the next sequence.
     *
     * @return the new sequence.
     */
    public long next()
    {
        return ++lastSequence;
    }

    /**
     * Last assigned (or applied) sequence.
     *
     * @return sequence, -1 before the first.
     */
    public long lastSequence()
    {
        return lastSequence;
    }

    /**
     * Record that a new order was accepted for sequencing and check it for duplicates.
     *
     * @param userId        user.
     * @param clientOrderId client order id.
     * @return true if the pair was seen before (the order must be rejected).
     */
    public boolean checkDuplicate(final long userId, final long clientOrderId)
    {
        return dedupEnabled && clientOrderId != 0 && !dedupWindow.add(userId, clientOrderId);
    }

    /**
     * Apply a command sequenced elsewhere (journal replay or replication).
     *
     * @param engineSequence sequence of the command.
     * @param newOrder       whether it is a new order.
     * @param duplicate      whether the original sequencer flagged it as a duplicate.
     * @param userId        user.
     * @param clientOrderId client order id.
     */
    public void observe(
        final long engineSequence,
        final boolean newOrder,
        final boolean duplicate,
        final long userId,
        final long clientOrderId)
    {
        if (engineSequence > lastSequence)
        {
            lastSequence = engineSequence;
        }
        if (dedupEnabled && newOrder && !duplicate && clientOrderId != 0)
        {
            dedupWindow.add(userId, clientOrderId);
        }
    }

    /**
     * Deduplication window (monitoring and tests).
     *
     * @return window.
     */
    public DedupWindow dedupWindow()
    {
        return dedupWindow;
    }

    /**
     * Serialise the sequencer state.
     *
     * @param buffer destination.
     * @param offset where to write.
     * @return bytes written.
     */
    public int write(final MutableDirectBuffer buffer, final int offset)
    {
        buffer.putInt(offset, STATE_VERSION, ORDER);
        buffer.putLong(offset + 4, lastSequence, ORDER);
        return 12 + dedupWindow.write(buffer, offset + 12);
    }

    /**
     * Restore the sequencer state.
     *
     * @param buffer source.
     * @param offset where the data starts.
     * @return bytes read.
     */
    public int read(final DirectBuffer buffer, final int offset)
    {
        final int version = buffer.getInt(offset, ORDER);
        if (version != STATE_VERSION)
        {
            throw new IllegalStateException("unsupported sequencer state version " + version);
        }
        lastSequence = buffer.getLong(offset + 4, ORDER);
        return 12 + dedupWindow.read(buffer, offset + 12);
    }
}
