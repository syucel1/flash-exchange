package com.flashexchange.engine.pipeline;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntArrayList;

/**
 * Re-usable list of encoded messages packed back to back in one growable buffer. Each ring slot owns one batch per
 * output stream; the matcher appends, the publisher drains and clears. Allocation free once warmed up.
 */
public final class MessageBatch
{
    private final ExpandableArrayBuffer buffer;
    private final IntArrayList lengths = new IntArrayList();
    private int limit;

    /**
     * Create a batch.
     *
     * @param initialCapacity of the backing buffer in bytes.
     */
    public MessageBatch(final int initialCapacity)
    {
        buffer = new ExpandableArrayBuffer(initialCapacity);
    }

    /**
     * Buffer to encode the next message into, starting at {@link #limit()}.
     *
     * @return the backing buffer (grows automatically on writes).
     */
    public MutableDirectBuffer buffer()
    {
        return buffer;
    }

    /**
     * Offset at which the next message must be encoded.
     *
     * @return current limit.
     */
    public int limit()
    {
        return limit;
    }

    /**
     * Record that a message of the given length has been encoded at {@link #limit()}.
     *
     * @param length of the encoded message.
     */
    public void commit(final int length)
    {
        if (length <= 0)
        {
            throw new IllegalArgumentException("length must be positive: " + length);
        }
        lengths.addInt(length);
        limit += length;
    }

    /**
     * Number of messages in the batch.
     *
     * @return count.
     */
    public int count()
    {
        return lengths.size();
    }

    /**
     * Length of a message.
     *
     * @param index of the message.
     * @return length in bytes.
     */
    public int length(final int index)
    {
        return lengths.getInt(index);
    }

    /**
     * Drain every message to a publisher in order.
     *
     * @param publisher destination.
     * @return number of messages the publisher accepted.
     */
    public int drainTo(final MessagePublisher publisher)
    {
        int offset = 0;
        int published = 0;
        for (int i = 0, size = lengths.size(); i < size; i++)
        {
            final int length = lengths.getInt(i);
            if (publisher.publish(buffer, offset, length))
            {
                published++;
            }
            offset += length;
        }
        return published;
    }

    /**
     * Remove all messages.
     */
    public void clear()
    {
        lengths.clear();
        limit = 0;
    }
}
