package com.flashexchange.protocol;

/**
 * Event: incremental L2 order book update (stream 3001, market data).
 * <p>
 * Emitted once per engine command that changed the book. Each entry carries the <em>new</em> total quantity and
 * order count of a price level; a quantity of zero means the level was removed. Consumers apply updates whose
 * {@link #bookSequence()} is exactly one greater than the last applied one and re-request a snapshot on a gap.
 */
public final class BookUpdateFlyweight extends BookLevelsFlyweight<BookUpdateFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 201;

    @Override
    public int templateId()
    {
        return TEMPLATE_ID;
    }

    /**
     * Start the level group. Must be called before {@link #putLevel(int, Side, long, long, int)}.
     *
     * @param count number of levels that will be written.
     * @return this for fluent use.
     */
    public BookUpdateFlyweight levelCount(final int count)
    {
        putGroupHeader(firstGroupOffset(), count);
        return this;
    }

    /**
     * Number of levels in the update.
     *
     * @return level count.
     */
    public int levelCount()
    {
        return groupCount(firstGroupOffset());
    }

    /**
     * Write a level.
     *
     * @param index      of the level within the update.
     * @param side       of the level.
     * @param price      of the level.
     * @param quantity   new total quantity (0 = level removed).
     * @param orderCount new order count.
     * @return this for fluent use.
     */
    public BookUpdateFlyweight putLevel(
        final int index, final Side side, final long price, final long quantity, final int orderCount)
    {
        putEntry(firstGroupOffset(), index, side, price, quantity, orderCount);
        return this;
    }

    /**
     * Side of a level.
     *
     * @param index of the level.
     * @return side.
     */
    public Side levelSide(final int index)
    {
        return entrySide(firstGroupOffset(), index);
    }

    /**
     * Price of a level.
     *
     * @param index of the level.
     * @return price.
     */
    public long levelPrice(final int index)
    {
        return entryPrice(firstGroupOffset(), index);
    }

    /**
     * Total quantity of a level.
     *
     * @param index of the level.
     * @return quantity (0 = removed).
     */
    public long levelQuantity(final int index)
    {
        return entryQuantity(firstGroupOffset(), index);
    }

    /**
     * Order count of a level.
     *
     * @param index of the level.
     * @return number of orders.
     */
    public int levelOrderCount(final int index)
    {
        return entryOrderCount(firstGroupOffset(), index);
    }

    @Override
    public int encodedLength()
    {
        return MessageHeader.LENGTH + actingBlockLength + groupLength(firstGroupOffset());
    }

    /**
     * Encoded length of an update with the given number of levels.
     *
     * @param levelCount number of levels.
     * @return encoded length in bytes.
     */
    public static int encodedLength(final int levelCount)
    {
        return MessageHeader.LENGTH + BLOCK_LENGTH + GROUP_HEADER_LENGTH + levelCount * ENTRY_LENGTH;
    }
}
