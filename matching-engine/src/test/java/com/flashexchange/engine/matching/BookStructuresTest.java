package com.flashexchange.engine.matching;

import com.flashexchange.protocol.Side;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BookStructuresTest
{
    static PriceLevel level(final Side side, final long price)
    {
        final PriceLevel level = new PriceLevel();
        level.init(side, price);
        return level;
    }

    static Order order(final long id, final long quantity)
    {
        final Order order = new Order();
        order.orderId = id;
        order.quantity = quantity;
        return order;
    }

    @Test
    void priceLevelIsFifoAndTracksAggregates()
    {
        final PriceLevel level = level(Side.BUY, 100);
        final Order a = order(1, 10);
        final Order b = order(2, 20);
        final Order c = order(3, 30);
        level.append(a);
        level.append(b);
        level.append(c);
        assertEquals(60, level.totalQuantity());
        assertEquals(3, level.orderCount());
        assertSame(a, level.head());
        assertSame(level, b.level);

        level.remove(b); // middle
        assertSame(c, a.next);
        assertSame(a, c.prev);
        assertNull(b.level);
        level.remove(a); // head
        assertSame(c, level.head());
        level.reduce(5);
        assertEquals(25, level.totalQuantity());
        c.filledQuantity = 5;
        level.remove(c); // tail and last
        assertTrue(level.isEmpty());
        assertNull(level.head());
        assertNull(level.tail);
        assertEquals(0, level.totalQuantity());

        level.reset();
        assertNull(level.side());
        assertEquals(0, level.price());
    }

    @Test
    void bookSideOrdersBestLast()
    {
        final BookSide bids = new BookSide(Side.BUY, 1);
        final BookSide asks = new BookSide(Side.SELL, 1);
        assertNull(bids.best());
        assertTrue(bids.isEmpty());
        final long[] prices = {105, 101, 110, 103, 99, 120, 100, 115, 102, 104, 98, 97, 96, 95, 94, 93, 92, 91};
        for (final long p : prices)
        {
            bids.insert(level(Side.BUY, p));
            asks.insert(level(Side.SELL, p));
        }
        assertEquals(prices.length, bids.levelCount());
        assertEquals(120, bids.best().price());
        assertEquals(91, asks.best().price());
        assertEquals(115, bids.levelAt(1).price());
        assertEquals(92, asks.levelAt(1).price());
        assertEquals(91, bids.levelAt(prices.length - 1).price());

        final PriceLevel mid = bids.level(103);
        bids.remove(mid);
        assertNull(bids.level(103));
        bids.remove(bids.best());
        assertEquals(115, bids.best().price());
        assertEquals(prices.length - 2, bids.levelCount());

        assertThrows(IllegalStateException.class, () -> new BookSide(Side.BUY, 1).remove(level(Side.BUY, 1)));
        assertThrows(IllegalStateException.class, () -> bids.remove(level(Side.BUY, 103)));
        assertThrows(IllegalStateException.class, () -> bids.remove(level(Side.BUY, 1_000)));
        assertThrows(IllegalStateException.class, () -> bids.remove(level(Side.BUY, 50)));
        assertThrows(IndexOutOfBoundsException.class, () -> bids.levelAt(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> bids.levelAt(prices.length));
    }

    @Test
    void crossesIsSideAware()
    {
        final BookSide asks = new BookSide(Side.SELL, 1);
        final BookSide bids = new BookSide(Side.BUY, 1);
        assertTrue(asks.crosses(100, 100));
        assertTrue(asks.crosses(99, 100));
        assertTrue(!asks.crosses(101, 100));
        assertTrue(bids.crosses(100, 100));
        assertTrue(bids.crosses(101, 100));
        assertTrue(!bids.crosses(99, 100));
    }

    @Test
    void poolPreallocatesRecyclesAndGrows()
    {
        final List<String> resets = new ArrayList<>();
        final ObjectPool<StringBuilder> pool = new ObjectPool<>(2, StringBuilder::new, sb -> resets.add("r"));
        assertEquals(2, pool.available());
        final StringBuilder a = pool.acquire();
        final StringBuilder b = pool.acquire();
        final StringBuilder c = pool.acquire();
        assertEquals(1, pool.allocationsBeyondInitial());
        pool.release(a);
        pool.release(b);
        pool.release(c);
        assertEquals(3, pool.available());
        assertEquals(3, resets.size());
        assertSame(c, pool.acquire());
        assertThrows(IllegalArgumentException.class, () -> new ObjectPool<>(0, StringBuilder::new, sb -> { }));
    }

    @Test
    void orderResetClearsEverything()
    {
        final Order o = order(5, 10);
        o.filledQuantity = 3;
        o.side = Side.BUY;
        o.level = level(Side.BUY, 1);
        o.reset();
        assertEquals(0, o.orderId());
        assertEquals(0, o.remainingQuantity());
        assertNull(o.side());
        assertTrue(!o.isResting());
    }

    @Test
    void instrumentValidation()
    {
        final Instrument ok = new Instrument(1, "EURUSD", 1, 1, 1, 10, 500);
        assertEquals("EURUSD", ok.symbol());
        assertThrows(NullPointerException.class, () -> new Instrument(1, null, 1, 1, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new Instrument(0, "X", 1, 1, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new Instrument(Instrument.MAX_ID + 1, "X", 1, 1, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new Instrument(1, "X", 0, 1, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new Instrument(1, "X", 1, 0, 1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new Instrument(1, "X", 1, 1, 0, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new Instrument(1, "X", 1, 1, 5, 4, 0));
        assertThrows(IllegalArgumentException.class, () -> new Instrument(1, "X", 1, 1, 1, 1, -1));
        assertThrows(IllegalArgumentException.class, () -> new Instrument(1, "X", 1, 1, 1, 1, 10_001));
    }
}
