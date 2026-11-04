package com.flashexchange.engine.matching;

import com.flashexchange.protocol.Side;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.Arrays;

/**
 * Limit order book of one instrument: bid and ask {@link BookSide}s, an order id index for O(1) cancels, the
 * per-instrument book and trade sequences, and a record of the levels changed by the command being processed (used
 * to build one incremental market data update per command).
 * <p>
 * Read accessors are public so event sinks can render market data; mutation is confined to {@link MatchingEngine}.
 */
public final class OrderBook
{
    private final Instrument instrument;
    private final BookSide bids;
    private final BookSide asks;
    private final Long2ObjectHashMap<Order> ordersById = new Long2ObjectHashMap<>();
    private long bookSequence;
    private long tradeSequence;

    private Side[] changedSides = new Side[16];
    private long[] changedPrices = new long[16];
    private int changedCount;

    OrderBook(final Instrument instrument, final int initialLevelCapacity)
    {
        this.instrument = instrument;
        this.bids = new BookSide(Side.BUY, initialLevelCapacity);
        this.asks = new BookSide(Side.SELL, initialLevelCapacity);
    }

    /**
     * Instrument of the book.
     *
     * @return instrument.
     */
    public Instrument instrument()
    {
        return instrument;
    }

    /**
     * Bid side.
     *
     * @return bids.
     */
    public BookSide bids()
    {
        return bids;
    }

    /**
     * Ask side.
     *
     * @return asks.
     */
    public BookSide asks()
    {
        return asks;
    }

    /**
     * Side of the book for the given side.
     *
     * @param side of the book.
     * @return the matching side.
     */
    public BookSide side(final Side side)
    {
        return side == Side.BUY ? bids : asks;
    }

    /**
     * Resting order by id.
     *
     * @param orderId engine order id.
     * @return the order or {@code null}.
     */
    public Order order(final long orderId)
    {
        return ordersById.get(orderId);
    }

    /**
     * Number of resting orders.
     *
     * @return order count.
     */
    public int orderCount()
    {
        return ordersById.size();
    }

    /**
     * Sequence of the last published book update.
     *
     * @return book sequence.
     */
    public long bookSequence()
    {
        return bookSequence;
    }

    long nextBookSequence()
    {
        return ++bookSequence;
    }

    long nextTradeId()
    {
        return ((long)instrument.id() << 40) | ++tradeSequence;
    }

    void addResting(final Order order, final ObjectPool<PriceLevel> levelPool)
    {
        final BookSide bookSide = side(order.side);
        PriceLevel level = bookSide.level(order.price);
        if (level == null)
        {
            level = levelPool.acquire();
            level.init(order.side, order.price);
            bookSide.insert(level);
        }
        level.append(order);
        ordersById.put(order.orderId, order);
        touch(order.side, order.price);
    }

    /**
     * Unlink a resting order (filled or cancelled) and drop its level if it became empty. The order itself is not
     * released to the pool.
     */
    void removeResting(final Order order, final ObjectPool<PriceLevel> levelPool)
    {
        final PriceLevel level = order.level;
        level.remove(order);
        ordersById.remove(order.orderId);
        touch(level.side, level.price);
        if (level.isEmpty())
        {
            side(level.side).remove(level);
            levelPool.release(level);
        }
    }

    void touch(final Side side, final long price)
    {
        for (int i = 0; i < changedCount; i++)
        {
            if (changedPrices[i] == price && changedSides[i] == side)
            {
                return;
            }
        }
        if (changedCount == changedPrices.length)
        {
            changedPrices = Arrays.copyOf(changedPrices, changedCount * 2);
            changedSides = Arrays.copyOf(changedSides, changedCount * 2);
        }
        changedSides[changedCount] = side;
        changedPrices[changedCount] = price;
        changedCount++;
    }

    void clearChanges()
    {
        changedCount = 0;
    }

    /**
     * Number of levels changed by the command being processed.
     *
     * @return changed level count.
     */
    public int changedLevelCount()
    {
        return changedCount;
    }

    /**
     * Side of a changed level.
     *
     * @param index of the change.
     * @return side.
     */
    public Side changedSide(final int index)
    {
        return changedSides[index];
    }

    /**
     * Price of a changed level.
     *
     * @param index of the change.
     * @return price.
     */
    public long changedPrice(final int index)
    {
        return changedPrices[index];
    }

    /**
     * Current total quantity at a price (0 if no level).
     *
     * @param side  of the book.
     * @param price of the level.
     * @return total quantity.
     */
    public long levelQuantity(final Side side, final long price)
    {
        final PriceLevel level = side(side).level(price);
        return level == null ? 0 : level.totalQuantity;
    }

    /**
     * Current order count at a price (0 if no level).
     *
     * @param side  of the book.
     * @param price of the level.
     * @return order count.
     */
    public int levelOrderCount(final Side side, final long price)
    {
        final PriceLevel level = side(side).level(price);
        return level == null ? 0 : level.orderCount;
    }
}
