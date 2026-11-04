package com.flashexchange.protocol;

/**
 * Event: full (or depth limited) L2 snapshot of one order book (stream 3001, market data).
 * <p>
 * Contains a bid group (best price first) followed by an ask group (best price first). {@link #bookSequence()} is
 * the sequence of the last {@link BookUpdateFlyweight} reflected in the snapshot. Encoding order matters: call
 * {@link #bidCount(int)}, write the bids, then {@link #askCount(int)} and write the asks.
 */
public final class BookSnapshotFlyweight extends BookLevelsFlyweight<BookSnapshotFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 202;

    @Override
    public int templateId()
    {
        return TEMPLATE_ID;
    }

    private int bidGroupOffset()
    {
        return firstGroupOffset();
    }

    private int askGroupOffset()
    {
        return bidGroupOffset() + groupLength(bidGroupOffset());
    }

    /**
     * Start the bid group.
     *
     * @param count number of bid levels.
     * @return this for fluent use.
     */
    public BookSnapshotFlyweight bidCount(final int count)
    {
        putGroupHeader(bidGroupOffset(), count);
        return this;
    }

    /**
     * Start the ask group. Must be called after the bid group has been sized.
     *
     * @param count number of ask levels.
     * @return this for fluent use.
     */
    public BookSnapshotFlyweight askCount(final int count)
    {
        putGroupHeader(askGroupOffset(), count);
        return this;
    }

    /**
     * Number of bid levels.
     *
     * @return count.
     */
    public int bidCount()
    {
        return groupCount(bidGroupOffset());
    }

    /**
     * Number of ask levels.
     *
     * @return count.
     */
    public int askCount()
    {
        return groupCount(askGroupOffset());
    }

    /**
     * Write a bid level.
     *
     * @param index      0 = best bid.
     * @param price      of the level.
     * @param quantity   total quantity.
     * @param orderCount number of orders.
     * @return this for fluent use.
     */
    public BookSnapshotFlyweight putBid(final int index, final long price, final long quantity, final int orderCount)
    {
        putEntry(bidGroupOffset(), index, Side.BUY, price, quantity, orderCount);
        return this;
    }

    /**
     * Write an ask level.
     *
     * @param index      0 = best ask.
     * @param price      of the level.
     * @param quantity   total quantity.
     * @param orderCount number of orders.
     * @return this for fluent use.
     */
    public BookSnapshotFlyweight putAsk(final int index, final long price, final long quantity, final int orderCount)
    {
        putEntry(askGroupOffset(), index, Side.SELL, price, quantity, orderCount);
        return this;
    }

    /**
     * Bid price.
     *
     * @param index 0 = best.
     * @return price.
     */
    public long bidPrice(final int index)
    {
        return entryPrice(bidGroupOffset(), index);
    }

    /**
     * Bid total quantity.
     *
     * @param index 0 = best.
     * @return quantity.
     */
    public long bidQuantity(final int index)
    {
        return entryQuantity(bidGroupOffset(), index);
    }

    /**
     * Bid order count.
     *
     * @param index 0 = best.
     * @return order count.
     */
    public int bidOrderCount(final int index)
    {
        return entryOrderCount(bidGroupOffset(), index);
    }

    /**
     * Ask price.
     *
     * @param index 0 = best.
     * @return price.
     */
    public long askPrice(final int index)
    {
        return entryPrice(askGroupOffset(), index);
    }

    /**
     * Ask total quantity.
     *
     * @param index 0 = best.
     * @return quantity.
     */
    public long askQuantity(final int index)
    {
        return entryQuantity(askGroupOffset(), index);
    }

    /**
     * Ask order count.
     *
     * @param index 0 = best.
     * @return order count.
     */
    public int askOrderCount(final int index)
    {
        return entryOrderCount(askGroupOffset(), index);
    }

    @Override
    public int encodedLength()
    {
        return MessageHeader.LENGTH + actingBlockLength + groupLength(bidGroupOffset()) +
            groupLength(askGroupOffset());
    }

    /**
     * Encoded length of a snapshot with the given number of levels.
     *
     * @param bids number of bid levels.
     * @param asks number of ask levels.
     * @return encoded length in bytes.
     */
    public static int encodedLength(final int bids, final int asks)
    {
        return MessageHeader.LENGTH + BLOCK_LENGTH + 2 * GROUP_HEADER_LENGTH + (bids + asks) * ENTRY_LENGTH;
    }
}
