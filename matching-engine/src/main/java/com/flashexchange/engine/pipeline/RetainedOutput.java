package com.flashexchange.engine.pipeline;

import org.agrona.ExpandableArrayBuffer;

/**
 * Output a standby computed but did not publish, kept until the primary reports it released (journal entries carry
 * the primary's released sequence). On promotion the remainder is re-published so no event is lost; consumers drop
 * the duplicates by engine sequence.
 * <p>
 * Entries are packed as {@code [engineSequence:8][stream:4][length:4][message]}. Single threaded (publisher).
 */
public final class RetainedOutput
{
    /** Stream index of private execution events. */
    public static final int EXECUTION = 0;

    /** Stream index of market data. */
    public static final int MARKET_DATA = 1;

    private static final int ENTRY_HEADER = 16;

    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64 * 1024);
    private int head;
    private int tail;
    private int count;

    /**
     * Retain the output of one command.
     *
     * @param engineSequence sequence of the command.
     * @param command        slot holding the output.
     */
    public void retain(final long engineSequence, final OrderCommand command)
    {
        append(engineSequence, EXECUTION, command.executionOutput());
        append(engineSequence, MARKET_DATA, command.marketDataOutput());
    }

    private void append(final long engineSequence, final int stream, final MessageBatch batch)
    {
        int offset = 0;
        for (int i = 0; i < batch.count(); i++)
        {
            final int length = batch.length(i);
            buffer.putLong(tail, engineSequence);
            buffer.putInt(tail + 8, stream);
            buffer.putInt(tail + 12, length);
            buffer.putBytes(tail + ENTRY_HEADER, batch.buffer(), offset, length);
            tail += ENTRY_HEADER + length;
            offset += length;
            count++;
        }
    }

    /**
     * Drop everything at or below a sequence (released by the primary).
     *
     * @param releasedSequence highest released sequence.
     */
    public void trim(final long releasedSequence)
    {
        while (head < tail && buffer.getLong(head) <= releasedSequence)
        {
            head += ENTRY_HEADER + buffer.getInt(head + 12);
            count--;
        }
        if (head == tail)
        {
            head = 0;
            tail = 0;
        }
        else if (head > buffer.capacity() / 2)
        {
            buffer.putBytes(0, buffer, head, tail - head);
            tail -= head;
            head = 0;
        }
    }

    /**
     * Publish every retained message in order, then clear.
     *
     * @param execution  destination of private events.
     * @param marketData destination of market data.
     * @return messages published.
     */
    public int drainTo(final MessagePublisher execution, final MessagePublisher marketData)
    {
        int published = 0;
        for (int pos = head; pos < tail; )
        {
            final int length = buffer.getInt(pos + 12);
            final MessagePublisher target = buffer.getInt(pos + 8) == EXECUTION ? execution : marketData;
            if (target.publish(buffer, pos + ENTRY_HEADER, length))
            {
                published++;
            }
            pos += ENTRY_HEADER + length;
        }
        clear();
        return published;
    }

    /**
     * Discard everything.
     */
    public void clear()
    {
        head = 0;
        tail = 0;
        count = 0;
    }

    /**
     * Messages retained.
     *
     * @return count.
     */
    public int size()
    {
        return count;
    }
}
