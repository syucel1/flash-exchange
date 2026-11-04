package com.flashexchange.engine.matching;

import com.flashexchange.protocol.Side;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.Arrays;

/**
 * One side of an order book: price levels kept in a sorted array ordered from worst to best price, so the best
 * level is at the end (O(1) access and removal), plus a hash index from price to level (O(1) lookup).
 * <p>
 * Inserting or removing a level that is not the best costs a binary search plus an array copy, which for realistic
 * books (hundreds to a few thousand levels) is cheaper than a pointer-based tree and is allocation free.
 */
public final class BookSide
{
    private final Side side;
    private final Long2ObjectHashMap<PriceLevel> levelByPrice = new Long2ObjectHashMap<>();
    private PriceLevel[] levels;
    private int size;

    BookSide(final Side side, final int initialCapacity)
    {
        this.side = side;
        this.levels = new PriceLevel[Math.max(16, initialCapacity)];
    }

    /**
     * Side of the book.
     *
     * @return side.
     */
    public Side side()
    {
        return side;
    }

    /**
     * Best level (highest bid / lowest ask).
     *
     * @return best level or {@code null} if empty.
     */
    public PriceLevel best()
    {
        return size == 0 ? null : levels[size - 1];
    }

    /**
     * Level at a given depth.
     *
     * @param depth 0 = best.
     * @return the level.
     * @throws IndexOutOfBoundsException if {@code depth >= levelCount()}.
     */
    public PriceLevel levelAt(final int depth)
    {
        if (depth < 0 || depth >= size)
        {
            throw new IndexOutOfBoundsException("depth=" + depth + " size=" + size);
        }
        return levels[size - 1 - depth];
    }

    /**
     * Level at a given price.
     *
     * @param price to look up.
     * @return the level or {@code null}.
     */
    public PriceLevel level(final long price)
    {
        return levelByPrice.get(price);
    }

    /**
     * Number of price levels.
     *
     * @return level count.
     */
    public int levelCount()
    {
        return size;
    }

    /**
     * Whether the side has no levels.
     *
     * @return true if empty.
     */
    public boolean isEmpty()
    {
        return size == 0;
    }

    /**
     * Whether {@code price} is at least as aggressive as a resting level at {@code levelPrice}, i.e. whether an
     * incoming order with limit {@code price} on the opposite side would match it.
     *
     * @param levelPrice price of a level on this side.
     * @param limit      limit of an incoming order on the opposite side.
     * @return true if they cross.
     */
    boolean crosses(final long levelPrice, final long limit)
    {
        return side == Side.SELL ? levelPrice <= limit : levelPrice >= limit;
    }

    private long key(final long price)
    {
        // ascending key order == worst to best
        return side == Side.BUY ? price : -price;
    }

    private int search(final long price)
    {
        final long key = key(price);
        int lo = 0;
        int hi = size;
        while (lo < hi)
        {
            final int mid = (lo + hi) >>> 1;
            if (key(levels[mid].price) < key)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }
        return lo;
    }

    void insert(final PriceLevel level)
    {
        if (size == levels.length)
        {
            levels = Arrays.copyOf(levels, levels.length * 2);
        }
        final int index = search(level.price);
        System.arraycopy(levels, index, levels, index + 1, size - index);
        levels[index] = level;
        size++;
        levelByPrice.put(level.price, level);
    }

    void remove(final PriceLevel level)
    {
        final int index;
        if (size > 0 && levels[size - 1] == level)
        {
            index = size - 1;
        }
        else
        {
            index = search(level.price);
            if (index >= size || levels[index] != level)
            {
                throw new IllegalStateException("level not on this side: " + level.price);
            }
        }
        System.arraycopy(levels, index + 1, levels, index, size - index - 1);
        levels[--size] = null;
        levelByPrice.remove(level.price);
    }
}
