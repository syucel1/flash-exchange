package com.flashexchange.engine.matching;

import com.flashexchange.engine.matching.RecordingEventSink.BookUpdate;
import com.flashexchange.engine.matching.RecordingEventSink.Level;
import com.flashexchange.engine.matching.RecordingEventSink.Report;
import com.flashexchange.engine.matching.RecordingEventSink.Trade;
import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential test: random order flow through the engine is compared against a deliberately simple reference
 * model (lists + sorting). Also checks book invariants after every command.
 */
class MatchingEngineRandomizedTest
{
    private static final int INSTRUMENT = 7;

    /** Naive reference order book. */
    static final class ReferenceBook
    {
        record RefOrder(long id, long user, Side side, long price, long[] remaining, long seq)
        {
        }

        record RefTrade(long buyId, long sellId, long price, long quantity)
        {
        }

        final List<RefOrder> resting = new ArrayList<>();
        final List<RefTrade> trades = new ArrayList<>();

        void submit(final long id, final long user, final Side side, final OrderType type, final TimeInForce tif,
            final long price, final long quantity, final long seq)
        {
            long remaining = quantity;
            final List<RefOrder> candidates = new ArrayList<>(resting.stream()
                .filter(o -> o.side() != side)
                .filter(o -> type == OrderType.MARKET || (side == Side.BUY ? o.price() <= price : o.price() >= price))
                .toList());
            candidates.sort(Comparator.<RefOrder>comparingLong(o -> side == Side.BUY ? o.price() : -o.price())
                .thenComparingLong(RefOrder::seq));
            for (final RefOrder maker : candidates)
            {
                if (remaining == 0)
                {
                    break;
                }
                final long q = Math.min(remaining, maker.remaining()[0]);
                trades.add(side == Side.BUY ? new RefTrade(id, maker.id(), maker.price(), q) :
                    new RefTrade(maker.id(), id, maker.price(), q));
                maker.remaining()[0] -= q;
                remaining -= q;
                if (maker.remaining()[0] == 0)
                {
                    resting.remove(maker);
                }
            }
            if (remaining > 0 && type == OrderType.LIMIT && tif == TimeInForce.GTC)
            {
                resting.add(new RefOrder(id, user, side, price, new long[] {remaining}, seq));
            }
        }

        boolean cancel(final long id, final long user)
        {
            return resting.removeIf(o -> o.id() == id && o.user() == user);
        }

        TreeMap<Long, long[]> levels(final Side side)
        {
            final TreeMap<Long, long[]> result = new TreeMap<>();
            for (final RefOrder o : resting)
            {
                if (o.side() == side)
                {
                    final long[] agg = result.computeIfAbsent(o.price(), p -> new long[2]);
                    agg[0] += o.remaining()[0];
                    agg[1]++;
                }
            }
            return result;
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 42, 1234, 98765})
    void engineMatchesReferenceModel(final long seed)
    {
        final Random random = new Random(seed);
        final RecordingEventSink sink = new RecordingEventSink();
        final MatchingEngine engine = new MatchingEngine(
            List.of(new Instrument(INSTRUMENT, "TST", 1, 1, 1, 1_000, 0)), sink, 64, 0);
        final ReferenceBook reference = new ReferenceBook();
        final List<long[]> live = new ArrayList<>();
        final Map<Long, Long> levelQuantities = new HashMap<>();

        for (long seq = 0; seq < 5_000; seq++)
        {
            final int action = random.nextInt(10);
            final long user = 1 + random.nextInt(5);
            if (action < 2 && !live.isEmpty())
            {
                final long[] target = live.remove(random.nextInt(live.size()));
                final long canceller = random.nextInt(10) == 0 ? user : target[1];
                engine.onCancelOrder(seq, seq, seq, canceller, INSTRUMENT, target[0]);
                final boolean refCancelled = reference.cancel(target[0], canceller);
                final boolean engineCancelled = sink.events.stream().anyMatch(e -> e instanceof Report r &&
                    r.orderId() == target[0] && r.execType() == ExecType.CANCELED);
                assertEquals(refCancelled, engineCancelled, "cancel outcome seq " + seq);
            }
            else
            {
                final Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
                final OrderType type = random.nextInt(8) == 0 ? OrderType.MARKET : OrderType.LIMIT;
                final TimeInForce tif = type == OrderType.MARKET ? TimeInForce.IOC :
                    random.nextInt(6) == 0 ? TimeInForce.IOC : TimeInForce.GTC;
                final long price = type == OrderType.MARKET ? 0 : 95 + random.nextInt(11);
                final long quantity = 1 + random.nextInt(50);
                final long id = seq + 1;
                engine.onNewOrder(seq, seq, id, id, user, INSTRUMENT, side.code(), type.code(), tif.code(),
                    SelfTradePrevention.NONE.code(), false, price, quantity);
                reference.submit(id, user, side, type, tif, price, quantity, seq);
                if (engine.book(INSTRUMENT).order(id) != null)
                {
                    live.add(new long[] {id, user});
                }
            }

            for (final BookUpdate update : sink.ofType(BookUpdate.class))
            {
                for (final Level level : update.levels())
                {
                    levelQuantities.put(level.side() == Side.BUY ? level.price() : -level.price(), level.quantity());
                }
            }
            assertTradesEqual(reference, sink);
            reference.trades.clear();
            sink.clear();
            assertBookEquals(reference, engine.book(INSTRUMENT), levelQuantities);
        }
    }

    private static void assertTradesEqual(final ReferenceBook reference, final RecordingEventSink sink)
    {
        final List<ReferenceBook.RefTrade> actual = sink.ofType(Trade.class).stream()
            .map(t -> new ReferenceBook.RefTrade(t.buyOrderId(), t.sellOrderId(), t.price(), t.quantity()))
            .toList();
        assertEquals(reference.trades, actual);
    }

    private static void assertBookEquals(
        final ReferenceBook reference, final OrderBook book, final Map<Long, Long> publishedQuantities)
    {
        for (final Side side : Side.values())
        {
            final TreeMap<Long, long[]> expected = reference.levels(side);
            final BookSide actual = book.side(side);
            assertEquals(expected.size(), actual.levelCount(), side + " level count");
            long previous = side == Side.BUY ? Long.MAX_VALUE : Long.MIN_VALUE;
            for (int depth = 0; depth < actual.levelCount(); depth++)
            {
                final PriceLevel level = actual.levelAt(depth);
                final long[] agg = expected.get(level.price());
                assertEquals(agg[0], level.totalQuantity(), "quantity at " + level.price());
                assertEquals(agg[1], level.orderCount(), "count at " + level.price());
                assertTrue(side == Side.BUY ? level.price() < previous : level.price() > previous, "sorted");
                previous = level.price();

                long sum = 0;
                int count = 0;
                for (Order o = level.head(); o != null; o = o.next)
                {
                    sum += o.remainingQuantity();
                    count++;
                }
                assertEquals(level.totalQuantity(), sum, "aggregate == sum of orders");
                assertEquals(level.orderCount(), count);
                assertEquals(level.totalQuantity(),
                    publishedQuantities.get(side == Side.BUY ? level.price() : -level.price()),
                    "published market data matches book");
            }
        }
        final PriceLevel bestBid = book.bids().best();
        final PriceLevel bestAsk = book.asks().best();
        if (bestBid != null && bestAsk != null)
        {
            assertTrue(bestBid.price() < bestAsk.price(), "book never crossed");
        }
    }
}
