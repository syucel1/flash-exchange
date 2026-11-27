package com.flashexchange.engine.ha;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Bounded, allocation-free set of the most recent {@code (userId, clientOrderId)} pairs, used by the sequencer to
 * reject resubmitted orders (for example a client resending after a failover). When full, the oldest pair is
 * evicted (FIFO). Open addressing with linear probing and backward-shift deletion.
 * <p>
 * The window is part of the engine state: it is written into snapshots and rebuilt identically on replay.
 */
public final class DedupWindow
{
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;

    private final int capacity;
    private final int mask;
    private final long[] tableUser;
    private final long[] tableClient;
    private final boolean[] used;
    private final long[] fifoUser;
    private final long[] fifoClient;
    private int fifoHead;
    private int size;

    /**
     * Create a window.
     *
     * @param capacity number of pairs remembered (at least 1).
     */
    public DedupWindow(final int capacity)
    {
        if (capacity < 1)
        {
            throw new IllegalArgumentException("capacity must be >= 1: " + capacity);
        }
        this.capacity = capacity;
        final int tableSize = Integer.highestOneBit(Math.max(4, capacity * 2 - 1)) << 1;
        this.mask = tableSize - 1;
        this.tableUser = new long[tableSize];
        this.tableClient = new long[tableSize];
        this.used = new boolean[tableSize];
        this.fifoUser = new long[capacity];
        this.fifoClient = new long[capacity];
    }

    /**
     * Number of pairs remembered.
     *
     * @return size.
     */
    public int size()
    {
        return size;
    }

    /**
     * Capacity.
     *
     * @return capacity.
     */
    public int capacity()
    {
        return capacity;
    }

    private int slot(final long userId, final long clientOrderId)
    {
        long h = userId * 0x9E3779B97F4A7C15L ^ clientOrderId * 0xC2B2AE3D27D4EB4FL;
        h ^= h >>> 29;
        return (int)h & mask;
    }

    /**
     * Whether the pair is in the window.
     *
     * @param userId        user.
     * @param clientOrderId client order id.
     * @return true if seen.
     */
    public boolean contains(final long userId, final long clientOrderId)
    {
        int i = slot(userId, clientOrderId);
        while (used[i])
        {
            if (tableUser[i] == userId && tableClient[i] == clientOrderId)
            {
                return true;
            }
            i = (i + 1) & mask;
        }
        return false;
    }

    /**
     * Add the pair unless present.
     *
     * @param userId        user.
     * @param clientOrderId client order id.
     * @return true if added, false if it was already present (a duplicate).
     */
    public boolean add(final long userId, final long clientOrderId)
    {
        if (contains(userId, clientOrderId))
        {
            return false;
        }
        if (size == capacity)
        {
            final int oldest = fifoHead;
            remove(fifoUser[oldest], fifoClient[oldest]);
        }
        insert(userId, clientOrderId);
        final int tail = (fifoHead + size) % capacity;
        fifoUser[tail] = userId;
        fifoClient[tail] = clientOrderId;
        size++;
        return true;
    }

    private void insert(final long userId, final long clientOrderId)
    {
        int i = slot(userId, clientOrderId);
        while (used[i])
        {
            i = (i + 1) & mask;
        }
        used[i] = true;
        tableUser[i] = userId;
        tableClient[i] = clientOrderId;
    }

    private void remove(final long userId, final long clientOrderId)
    {
        int i = slot(userId, clientOrderId);
        while (!(tableUser[i] == userId && tableClient[i] == clientOrderId))
        {
            i = (i + 1) & mask;
        }
        used[i] = false;
        // backward-shift following entries of the probe chain
        int j = (i + 1) & mask;
        while (used[j])
        {
            final int home = slot(tableUser[j], tableClient[j]);
            final boolean shift = i <= j ? (home <= i || home > j) : (home <= i && home > j);
            if (shift)
            {
                used[i] = true;
                tableUser[i] = tableUser[j];
                tableClient[i] = tableClient[j];
                used[j] = false;
                i = j;
            }
            j = (j + 1) & mask;
        }
        fifoHead = (fifoHead + 1) % capacity;
        size--;
    }

    /**
     * Forget everything.
     */
    public void clear()
    {
        Arrays.fill(used, false);
        fifoHead = 0;
        size = 0;
    }

    /**
     * Serialise the window, oldest first.
     *
     * @param buffer destination.
     * @param offset where to write.
     * @return bytes written.
     */
    public int write(final MutableDirectBuffer buffer, final int offset)
    {
        buffer.putInt(offset, size, ORDER);
        int pos = offset + 4;
        for (int k = 0; k < size; k++)
        {
            final int idx = (fifoHead + k) % capacity;
            buffer.putLong(pos, fifoUser[idx], ORDER);
            buffer.putLong(pos + 8, fifoClient[idx], ORDER);
            pos += 16;
        }
        return pos - offset;
    }

    /**
     * Replace the content with a serialised window. Entries beyond the capacity are dropped oldest first.
     *
     * @param buffer source.
     * @param offset where the data starts.
     * @return bytes read.
     */
    public int read(final DirectBuffer buffer, final int offset)
    {
        clear();
        final int count = buffer.getInt(offset, ORDER);
        int pos = offset + 4;
        for (int k = 0; k < count; k++)
        {
            add(buffer.getLong(pos, ORDER), buffer.getLong(pos + 8, ORDER));
            pos += 16;
        }
        return pos - offset;
    }
}
