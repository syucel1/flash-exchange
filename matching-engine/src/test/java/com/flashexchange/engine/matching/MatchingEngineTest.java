package com.flashexchange.engine.matching;

import com.flashexchange.engine.matching.RecordingEventSink.BookUpdate;
import com.flashexchange.engine.matching.RecordingEventSink.CancelReject;
import com.flashexchange.engine.matching.RecordingEventSink.Event;
import com.flashexchange.engine.matching.RecordingEventSink.Level;
import com.flashexchange.engine.matching.RecordingEventSink.Reject;
import com.flashexchange.engine.matching.RecordingEventSink.Report;
import com.flashexchange.engine.matching.RecordingEventSink.Snapshot;
import com.flashexchange.engine.matching.RecordingEventSink.Trade;
import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.Liquidity;
import com.flashexchange.protocol.OrderStatus;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static com.flashexchange.protocol.Side.BUY;
import static com.flashexchange.protocol.Side.SELL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatchingEngineTest
{
    static final int EURUSD = 1;
    static final int GBPUSD = 2;
    static final int NO_COLLAR = 3;
    static final long ALICE = 100;
    static final long BOB = 200;
    static final long CAROL = 300;

    final RecordingEventSink sink = new RecordingEventSink();
    MatchingEngine engine;
    long sequence;

    @BeforeEach
    void setUp()
    {
        engine = new MatchingEngine(List.of(
            new Instrument(EURUSD, "EURUSD", 5, 100, 100, 10_000_000, 500),
            new Instrument(GBPUSD, "GBPUSD", 1, 1, 1, 1_000_000, 500),
            new Instrument(NO_COLLAR, "USDJPY", 1, 1, 1, 1_000_000, 0)), sink, 4, 0);
    }

    long submit(
        final int instrumentId, final long userId, final Side side, final OrderType type, final TimeInForce tif,
        final SelfTradePrevention stp, final boolean postOnly, final long price, final long quantity)
    {
        return submitRaw(instrumentId, userId, side.code(), type.code(), tif.code(), stp.code(), postOnly, price,
            quantity);
    }

    long submitRaw(
        final int instrumentId, final long userId, final byte side, final byte type, final byte tif,
        final byte stp, final boolean postOnly, final long price, final long quantity)
    {
        final long seq = sequence++;
        final long orderId = seq + 1;
        engine.onNewOrder(seq, seq * 10, orderId, 1000 + seq, userId, instrumentId, side, type, tif, stp,
            postOnly, price, quantity);
        return orderId;
    }

    long limit(final long userId, final Side side, final long price, final long quantity)
    {
        return submit(GBPUSD, userId, side, OrderType.LIMIT, TimeInForce.GTC, SelfTradePrevention.NONE, false,
            price, quantity);
    }

    long market(final long userId, final Side side, final long quantity)
    {
        return submit(GBPUSD, userId, side, OrderType.MARKET, TimeInForce.IOC, SelfTradePrevention.NONE, false,
            0, quantity);
    }

    void cancel(final long userId, final int instrumentId, final long orderId)
    {
        final long seq = sequence++;
        engine.onCancelOrder(seq, seq * 10, 5000 + seq, userId, instrumentId, orderId);
    }

    OrderBook book()
    {
        return engine.book(GBPUSD);
    }

    List<Report> reports()
    {
        return sink.ofType(Report.class);
    }

    List<Trade> trades()
    {
        return sink.ofType(Trade.class);
    }

    BookUpdate lastBookUpdate()
    {
        final List<BookUpdate> updates = sink.ofType(BookUpdate.class);
        return updates.get(updates.size() - 1);
    }

    static void assertReport(
        final Report r, final long orderId, final ExecType type, final OrderStatus status, final long cumQty,
        final long remaining)
    {
        assertEquals(orderId, r.orderId(), "orderId");
        assertEquals(type, r.execType(), "execType");
        assertEquals(status, r.status(), "status");
        assertEquals(cumQty, r.cumulativeQuantity(), "cumQty");
        assertEquals(remaining, r.remainingQuantity(), "remaining");
    }

    @Nested
    class LimitOrders
    {
        @Test
        void restingOrderIsAcknowledgedAndPublished()
        {
            final long id = limit(ALICE, BUY, 100, 10);

            assertEquals(2, sink.events.size());
            final Report ack = reports().get(0);
            assertReport(ack, id, ExecType.NEW, OrderStatus.NEW, 0, 10);
            assertEquals(1000, ack.clientOrderId());
            assertEquals(ALICE, ack.userId());
            assertEquals(Liquidity.NONE, ack.liquidity());
            assertEquals(Reason.NONE, ack.reason());
            assertEquals(new BookUpdate(0, GBPUSD, 1, List.of(new Level(BUY, 100, 10, 1))), lastBookUpdate());

            final Order resting = book().order(id);
            assertNotNull(resting);
            assertTrue(resting.isResting());
            assertEquals(100, book().bids().best().price());
            assertEquals(1, book().orderCount());
        }

        @Test
        void fullMatchAtMakerPrice()
        {
            final long maker = limit(ALICE, SELL, 100, 10);
            sink.clear();
            final long taker = limit(BOB, BUY, 105, 10);

            final List<Event> e = sink.events;
            assertEquals(5, e.size());
            assertReport((Report)e.get(0), taker, ExecType.NEW, OrderStatus.NEW, 0, 10);
            final Trade trade = (Trade)e.get(1);
            assertEquals(100, trade.price(), "executes at maker price");
            assertEquals(10, trade.quantity());
            assertEquals(taker, trade.buyOrderId());
            assertEquals(maker, trade.sellOrderId());
            assertEquals(BOB, trade.buyUserId());
            assertEquals(ALICE, trade.sellUserId());
            assertEquals(BUY, trade.aggressor());
            assertEquals(((long)GBPUSD << 40) | 1, trade.tradeId());

            final Report makerFill = (Report)e.get(2);
            assertReport(makerFill, maker, ExecType.TRADE, OrderStatus.FILLED, 10, 0);
            assertEquals(Liquidity.MAKER, makerFill.liquidity());
            assertEquals(100, makerFill.lastPrice());
            assertEquals(10, makerFill.lastQuantity());
            assertEquals(trade.tradeId(), makerFill.tradeId());

            final Report takerFill = (Report)e.get(3);
            assertReport(takerFill, taker, ExecType.TRADE, OrderStatus.FILLED, 10, 0);
            assertEquals(Liquidity.TAKER, takerFill.liquidity());

            assertEquals(new BookUpdate(1, GBPUSD, 2, List.of(new Level(SELL, 100, 0, 0))), e.get(4));
            assertTrue(book().asks().isEmpty());
            assertTrue(book().bids().isEmpty());
            assertEquals(0, book().orderCount());
        }

        @Test
        void partialFillTakerRestsRemainder()
        {
            limit(ALICE, SELL, 100, 4);
            sink.clear();
            final long taker = limit(BOB, BUY, 100, 10);

            final Report fill = reports().get(2);
            assertReport(fill, taker, ExecType.TRADE, OrderStatus.PARTIALLY_FILLED, 4, 6);
            assertEquals(4, fill.lastQuantity());
            assertEquals(List.of(new Level(SELL, 100, 0, 0), new Level(BUY, 100, 6, 1)), lastBookUpdate().levels());
            final Order resting = book().order(taker);
            assertEquals(4, resting.filledQuantity());
            assertEquals(6, resting.remainingQuantity());
            assertEquals(1, book().bids().levelCount());
        }

        @Test
        void partialFillMakerRemainsWithPriority()
        {
            final long maker = limit(ALICE, SELL, 100, 10);
            limit(CAROL, SELL, 100, 5);
            sink.clear();
            limit(BOB, BUY, 100, 3);

            assertReport(reports().get(1), maker, ExecType.TRADE, OrderStatus.PARTIALLY_FILLED, 3, 7);
            assertEquals(List.of(new Level(SELL, 100, 12, 2)), lastBookUpdate().levels());
            assertEquals(maker, book().asks().best().head().orderId(), "partially filled maker keeps priority");
        }

        @Test
        void priceTimePriorityAcrossAndWithinLevels()
        {
            final long a1 = limit(ALICE, SELL, 101, 5);
            final long b1 = limit(BOB, SELL, 100, 5);
            final long c1 = limit(CAROL, SELL, 100, 5);
            final long a2 = limit(ALICE, SELL, 102, 5);
            sink.clear();

            final long taker = limit(999, BUY, 102, 18);

            final List<Trade> t = trades();
            assertEquals(4, t.size());
            assertEquals(List.of(b1, c1, a1, a2), t.stream().map(Trade::sellOrderId).toList());
            assertEquals(List.of(100L, 100L, 101L, 102L), t.stream().map(Trade::price).toList());
            assertEquals(List.of(5L, 5L, 5L, 3L), t.stream().map(Trade::quantity).toList());
            assertEquals(List.of(1L, 2L, 3L, 4L), t.stream().map(x -> x.tradeId() & 0xFFFFFFFFFFL).toList());
            assertNull(book().order(taker), "fully filled");
            assertEquals(2, book().asks().best().totalQuantity());
            assertEquals(List.of(new Level(SELL, 100, 0, 0), new Level(SELL, 101, 0, 0), new Level(SELL, 102, 2, 1)),
                lastBookUpdate().levels());
        }

        @Test
        void limitStopsAtLimitPriceAndRests()
        {
            limit(ALICE, SELL, 100, 5);
            limit(ALICE, SELL, 103, 5);
            sink.clear();
            final long taker = limit(BOB, BUY, 101, 8);

            assertEquals(1, trades().size());
            assertEquals(101, book().bids().best().price());
            assertEquals(3, book().order(taker).remainingQuantity());
            assertEquals(103, book().asks().best().price());
        }

        @Test
        void sellTakerHitsHighestBidFirst()
        {
            final long low = limit(ALICE, BUY, 98, 5);
            final long high = limit(BOB, BUY, 99, 5);
            sink.clear();
            final long taker = limit(CAROL, SELL, 97, 7);

            final List<Trade> t = trades();
            assertEquals(List.of(high, low), t.stream().map(Trade::buyOrderId).toList());
            assertEquals(List.of(99L, 98L), t.stream().map(Trade::price).toList());
            assertEquals(SELL, t.get(0).aggressor());
            assertEquals(taker, t.get(0).sellOrderId());
            assertEquals(3, book().order(low).remainingQuantity());
        }

        @Test
        void sweepOfManyLevelsPublishesOneUpdateWithEveryLevel()
        {
            for (int i = 0; i < 25; i++)
            {
                limit(ALICE, SELL, 1000 + i, 2);
            }
            sink.clear();
            limit(BOB, BUY, 1024, 60);
            assertEquals(25, trades().size());
            final List<Level> levels = lastBookUpdate().levels();
            assertEquals(26, levels.size(), "25 removed ask levels + the resting bid");
            assertEquals(new Level(BUY, 1024, 10, 1), levels.get(25));
            assertEquals(1, sink.ofType(BookUpdate.class).size());
        }

        @Test
        void manyLevelsKeepSortedOrderAndCancelFromMiddle()
        {
            for (int i = 0; i < 40; i++)
            {
                limit(ALICE, BUY, 1000 - (i * 7L % 40) * 3, 1);
                limit(ALICE, SELL, 2000 + (i * 11L % 40) * 3, 1);
            }
            assertEquals(40, book().bids().levelCount());
            assertEquals(40, book().asks().levelCount());
            for (int d = 1; d < 40; d++)
            {
                assertTrue(book().bids().levelAt(d - 1).price() > book().bids().levelAt(d).price());
                assertTrue(book().asks().levelAt(d - 1).price() < book().asks().levelAt(d).price());
            }

            final long middle = book().bids().levelAt(20).head().orderId();
            final long middlePrice = book().bids().levelAt(20).price();
            cancel(ALICE, GBPUSD, middle);
            assertEquals(39, book().bids().levelCount());
            assertNull(book().bids().level(middlePrice));
        }
    }

    @Nested
    class MarketOrders
    {
        @Test
        void fillsAcrossLevelsAndCancelsRemainderWhenBookExhausted()
        {
            limit(ALICE, SELL, 100, 5);
            limit(BOB, SELL, 101, 5);
            sink.clear();
            final long taker = market(CAROL, BUY, 15);

            final List<Report> r = reports();
            assertEquals(ExecType.NEW, r.get(0).execType());
            assertEquals(0, r.get(0).price(), "market orders carry no price");
            assertEquals(2, trades().size());
            final Report cancel = r.get(r.size() - 1);
            assertReport(cancel, taker, ExecType.CANCELED, OrderStatus.CANCELED, 10, 5);
            assertEquals(Reason.NO_LIQUIDITY, cancel.reason());
            assertTrue(book().asks().isEmpty());
            assertNull(book().order(taker), "market orders never rest");
        }

        @Test
        void noLiquidityCancelsWithoutBookUpdate()
        {
            final long taker = market(CAROL, SELL, 15);
            assertEquals(2, sink.events.size());
            assertReport((Report)sink.events.get(1), taker, ExecType.CANCELED, OrderStatus.CANCELED, 0, 15);
            assertEquals(Reason.NO_LIQUIDITY, ((Report)sink.events.get(1)).reason());
            assertTrue(sink.ofType(BookUpdate.class).isEmpty());
        }

        @Test
        void buyCollarStopsSweep()
        {
            limit(ALICE, SELL, 10_000, 5);
            limit(ALICE, SELL, 10_500, 5);   // exactly +5%: inside the collar
            limit(ALICE, SELL, 10_501, 5);   // outside
            sink.clear();
            final long taker = market(BOB, BUY, 20);

            assertEquals(2, trades().size());
            final Report cancel = reports().get(reports().size() - 1);
            assertEquals(Reason.PRICE_COLLAR, cancel.reason());
            assertEquals(10, cancel.cumulativeQuantity());
            assertEquals(10_501, book().asks().best().price());
            assertNull(book().order(taker));
        }

        @Test
        void sellCollarStopsSweep()
        {
            limit(ALICE, BUY, 10_000, 5);
            limit(ALICE, BUY, 9_500, 5);
            limit(ALICE, BUY, 9_499, 5);
            sink.clear();
            market(BOB, SELL, 20);

            assertEquals(2, trades().size());
            assertEquals(Reason.PRICE_COLLAR, reports().get(reports().size() - 1).reason());
        }

        @Test
        void noCollarSweepsWholeBook()
        {
            submit(NO_COLLAR, ALICE, SELL, OrderType.LIMIT, TimeInForce.GTC, SelfTradePrevention.NONE, false, 100, 1);
            submit(NO_COLLAR, ALICE, SELL, OrderType.LIMIT, TimeInForce.GTC, SelfTradePrevention.NONE, false,
                900_000, 1);
            sink.clear();
            submit(NO_COLLAR, BOB, BUY, OrderType.MARKET, TimeInForce.IOC, SelfTradePrevention.NONE, false, 0, 2);
            assertEquals(2, trades().size());
            assertTrue(engine.book(NO_COLLAR).asks().isEmpty());
        }

        @Test
        void marketGtcIsTreatedAsIoc()
        {
            limit(ALICE, SELL, 100, 5);
            sink.clear();
            final long taker = submit(GBPUSD, BOB, BUY, OrderType.MARKET, TimeInForce.GTC,
                SelfTradePrevention.NONE, false, 0, 8);
            assertTrue(book().bids().isEmpty(), "market GTC never rests");
            final Report cancel = reports().get(reports().size() - 1);
            assertReport(cancel, taker, ExecType.CANCELED, OrderStatus.CANCELED, 5, 3);
        }

        @Test
        void marketFokFillsOrKills()
        {
            limit(ALICE, SELL, 100, 5);
            sink.clear();
            submit(GBPUSD, BOB, BUY, OrderType.MARKET, TimeInForce.FOK, SelfTradePrevention.NONE, false, 0, 6);
            assertTrue(trades().isEmpty());
            assertEquals(Reason.FOK_NOT_FILLABLE, reports().get(1).reason());

            sink.clear();
            submit(GBPUSD, BOB, BUY, OrderType.MARKET, TimeInForce.FOK, SelfTradePrevention.NONE, false, 0, 5);
            assertEquals(1, trades().size());
        }
    }

    @Nested
    class TimeInForceRules
    {
        @Test
        void iocCancelsRemainder()
        {
            limit(ALICE, SELL, 100, 4);
            sink.clear();
            final long taker = submit(GBPUSD, BOB, BUY, OrderType.LIMIT, TimeInForce.IOC,
                SelfTradePrevention.NONE, false, 100, 10);
            final Report cancel = reports().get(reports().size() - 1);
            assertReport(cancel, taker, ExecType.CANCELED, OrderStatus.CANCELED, 4, 6);
            assertEquals(Reason.IOC_REMAINDER, cancel.reason());
            assertTrue(book().bids().isEmpty(), "IOC never rests");
        }

        @Test
        void iocWithoutCrossCancelsEverything()
        {
            submit(GBPUSD, BOB, BUY, OrderType.LIMIT, TimeInForce.IOC, SelfTradePrevention.NONE, false, 100, 10);
            assertEquals(Reason.IOC_REMAINDER, reports().get(1).reason());
            assertTrue(sink.ofType(BookUpdate.class).isEmpty());
        }

        @Test
        void fokFullyFillableExecutes()
        {
            limit(ALICE, SELL, 100, 4);
            limit(CAROL, SELL, 101, 6);
            sink.clear();
            submit(GBPUSD, BOB, BUY, OrderType.LIMIT, TimeInForce.FOK, SelfTradePrevention.NONE, false, 101, 10);
            assertEquals(2, trades().size());
            assertEquals(OrderStatus.FILLED, reports().get(reports().size() - 1).status());
        }

        @Test
        void fokNotFillableLeavesBookUntouched()
        {
            limit(ALICE, SELL, 100, 4);
            limit(CAROL, SELL, 102, 6);
            sink.clear();
            final long taker = submit(GBPUSD, BOB, BUY, OrderType.LIMIT, TimeInForce.FOK,
                SelfTradePrevention.NONE, false, 101, 10);
            assertEquals(2, sink.events.size());
            assertReport(reports().get(1), taker, ExecType.CANCELED, OrderStatus.CANCELED, 0, 10);
            assertEquals(Reason.FOK_NOT_FILLABLE, reports().get(1).reason());
            assertEquals(4, book().asks().best().totalQuantity());
        }
    }

    @Nested
    class PostOnly
    {
        @Test
        void restsWhenNotCrossing()
        {
            limit(ALICE, SELL, 100, 4);
            sink.clear();
            final long id = submit(GBPUSD, BOB, BUY, OrderType.LIMIT, TimeInForce.GTC, SelfTradePrevention.NONE,
                true, 99, 10);
            assertNotNull(book().order(id));
            assertEquals(ExecType.NEW, reports().get(0).execType());
        }

        @Test
        void rejectedWhenItWouldTake()
        {
            limit(ALICE, SELL, 100, 4);
            sink.clear();
            submit(GBPUSD, BOB, BUY, OrderType.LIMIT, TimeInForce.GTC, SelfTradePrevention.NONE, true, 100, 10);
            assertEquals(Reason.POST_ONLY_WOULD_TAKE, sink.ofType(Reject.class).get(0).reason());
            assertEquals(1, sink.events.size());
        }

        @Test
        void restsOnEmptyOppositeSide()
        {
            final long id = submit(GBPUSD, BOB, SELL, OrderType.LIMIT, TimeInForce.GTC, SelfTradePrevention.NONE,
                true, 99, 10);
            assertNotNull(book().order(id));
        }

        @Test
        void notAllowedOnMarketOrders()
        {
            submit(GBPUSD, BOB, BUY, OrderType.MARKET, TimeInForce.IOC, SelfTradePrevention.NONE, true, 0, 10);
            assertEquals(Reason.POST_ONLY_NOT_ALLOWED, sink.ofType(Reject.class).get(0).reason());
        }
    }

    @Nested
    class Validation
    {
        void assertRejected(final Reason expected)
        {
            assertEquals(1, sink.events.size(), sink.events.toString());
            final Reject reject = assertInstanceOf(Reject.class, sink.events.get(0));
            assertEquals(expected, reject.reason());
            assertEquals(0, book().orderCount());
            sink.clear();
        }

        @Test
        void rejectsInvalidOrders()
        {
            final byte buy = BUY.code();
            final byte lim = OrderType.LIMIT.code();
            final byte gtc = TimeInForce.GTC.code();
            final byte none = SelfTradePrevention.NONE.code();

            submitRaw(99, ALICE, buy, lim, gtc, none, false, 100, 10);
            assertRejected(Reason.UNKNOWN_INSTRUMENT);
            submitRaw(GBPUSD, ALICE, (byte)9, lim, gtc, none, false, 100, 10);
            assertRejected(Reason.INVALID_SIDE);
            submitRaw(GBPUSD, ALICE, buy, (byte)0, gtc, none, false, 100, 10);
            assertRejected(Reason.INVALID_ORDER_TYPE);
            submitRaw(GBPUSD, ALICE, buy, lim, (byte)7, none, false, 100, 10);
            assertRejected(Reason.INVALID_TIME_IN_FORCE);
            submitRaw(GBPUSD, ALICE, buy, lim, gtc, (byte)9, false, 100, 10);
            assertRejected(Reason.INVALID_SELF_TRADE_PREVENTION);
            submitRaw(GBPUSD, ALICE, buy, lim, gtc, none, false, 100, 0);
            assertRejected(Reason.INVALID_QUANTITY);
            submitRaw(GBPUSD, ALICE, buy, lim, gtc, none, false, 100, -5);
            assertRejected(Reason.INVALID_QUANTITY);
            submitRaw(GBPUSD, ALICE, buy, lim, gtc, none, false, 100, 1_000_001);
            assertRejected(Reason.ABOVE_MAX_QUANTITY);
            submitRaw(GBPUSD, ALICE, buy, lim, gtc, none, false, 0, 10);
            assertRejected(Reason.INVALID_PRICE);
            submitRaw(GBPUSD, ALICE, buy, lim, gtc, none, false, -1, 10);
            assertRejected(Reason.INVALID_PRICE);
            submitRaw(GBPUSD, ALICE, buy, OrderType.MARKET.code(), gtc, none, false, 100, 10);
            assertRejected(Reason.INVALID_PRICE);

            // EURUSD: tick 5, lot 100, min 100
            submitRaw(EURUSD, ALICE, buy, lim, gtc, none, false, 100, 150);
            assertRejected(Reason.INVALID_QUANTITY);
            submitRaw(EURUSD, ALICE, buy, lim, gtc, none, false, 101, 100);
            assertRejected(Reason.INVALID_PRICE);
        }

        @Test
        void belowMinimumQuantity()
        {
            final MatchingEngine e = new MatchingEngine(
                List.of(new Instrument(5, "X", 1, 10, 50, 1000, 0)), sink, 1, 0);
            e.onNewOrder(0, 0, 1, 1, ALICE, 5, BUY.code(), OrderType.LIMIT.code(), TimeInForce.GTC.code(),
                SelfTradePrevention.NONE.code(), false, 10, 40);
            final Reject reject = sink.ofType(Reject.class).get(0);
            assertEquals(Reason.BELOW_MIN_QUANTITY, reject.reason());
            assertEquals(1, reject.orderId());
            assertEquals(40, reject.quantity());
            assertEquals(BUY.code(), reject.side());
        }
    }

    @Nested
    class Cancels
    {
        @Test
        void cancelRestingOrder()
        {
            final long id = limit(ALICE, BUY, 100, 10);
            limit(BOB, BUY, 100, 5);
            sink.clear();
            cancel(ALICE, GBPUSD, id);

            assertEquals(2, sink.events.size());
            final Report r = reports().get(0);
            assertReport(r, id, ExecType.CANCELED, OrderStatus.CANCELED, 0, 10);
            assertEquals(Reason.USER_CANCEL, r.reason());
            assertEquals(List.of(new Level(BUY, 100, 5, 1)), lastBookUpdate().levels());
            assertNull(book().order(id));
        }

        @Test
        void cancelPartiallyFilledOrderKeepsCumulativeQuantity()
        {
            final long id = limit(ALICE, BUY, 100, 10);
            limit(BOB, SELL, 100, 3);
            sink.clear();
            cancel(ALICE, GBPUSD, id);
            assertReport(reports().get(0), id, ExecType.CANCELED, OrderStatus.CANCELED, 3, 7);
            assertEquals(List.of(new Level(BUY, 100, 0, 0)), lastBookUpdate().levels());
        }

        @Test
        void rejectsUnknownForeignAndFilledOrders()
        {
            final long id = limit(ALICE, BUY, 100, 10);
            sink.clear();

            cancel(ALICE, GBPUSD, 424242);
            cancel(BOB, GBPUSD, id);
            cancel(ALICE, 77, id);
            limit(BOB, SELL, 100, 10);
            cancel(ALICE, GBPUSD, id);

            final List<CancelReject> rejects = sink.ofType(CancelReject.class);
            assertEquals(List.of(Reason.UNKNOWN_ORDER, Reason.NOT_ORDER_OWNER, Reason.UNKNOWN_INSTRUMENT,
                Reason.UNKNOWN_ORDER), rejects.stream().map(CancelReject::reason).toList());
            assertEquals(BOB, rejects.get(1).userId());
            assertEquals(id, rejects.get(1).orderId());
            assertEquals(5000 + 2, rejects.get(1).clientOrderId());
        }
    }

    @Nested
    class SelfTradePreventionModes
    {
        long ownAsk;
        long otherAsk;

        @BeforeEach
        void bookWithOwnOrderFirst()
        {
            ownAsk = limit(ALICE, SELL, 100, 5);
            otherAsk = limit(BOB, SELL, 100, 5);
            sink.clear();
        }

        long aliceBuys(final SelfTradePrevention stp, final TimeInForce tif)
        {
            return submit(GBPUSD, ALICE, BUY, OrderType.LIMIT, tif, stp, false, 100, 8);
        }

        @Test
        void noneAllowsSelfTrade()
        {
            aliceBuys(SelfTradePrevention.NONE, TimeInForce.GTC);
            final Trade first = trades().get(0);
            assertEquals(ALICE, first.buyUserId());
            assertEquals(ALICE, first.sellUserId());
        }

        @Test
        void cancelTakerKeepsRestingOrder()
        {
            final long taker = aliceBuys(SelfTradePrevention.CANCEL_TAKER, TimeInForce.GTC);
            assertTrue(trades().isEmpty());
            final Report cancel = reports().get(1);
            assertReport(cancel, taker, ExecType.CANCELED, OrderStatus.CANCELED, 0, 8);
            assertEquals(Reason.SELF_TRADE_PREVENTION, cancel.reason());
            assertNotNull(book().order(ownAsk));
            assertNull(book().order(taker), "cancelled taker does not rest");
            assertTrue(sink.ofType(BookUpdate.class).isEmpty());
        }

        @Test
        void cancelMakerRemovesOwnOrderAndContinues()
        {
            final long taker = aliceBuys(SelfTradePrevention.CANCEL_MAKER, TimeInForce.GTC);
            final Report makerCancel = reports().get(1);
            assertReport(makerCancel, ownAsk, ExecType.CANCELED, OrderStatus.CANCELED, 0, 5);
            assertEquals(Reason.SELF_TRADE_PREVENTION, makerCancel.reason());
            assertEquals(1, trades().size());
            assertEquals(otherAsk, trades().get(0).sellOrderId());
            assertEquals(3, book().order(taker).remainingQuantity());
        }

        @Test
        void cancelBothRemovesBoth()
        {
            final long taker = aliceBuys(SelfTradePrevention.CANCEL_BOTH, TimeInForce.GTC);
            assertTrue(trades().isEmpty());
            assertEquals(Reason.SELF_TRADE_PREVENTION, reports().get(1).reason());
            assertEquals(ownAsk, reports().get(1).orderId());
            assertEquals(taker, reports().get(2).orderId());
            assertEquals(Reason.SELF_TRADE_PREVENTION, reports().get(2).reason());
            assertNull(book().order(ownAsk));
            assertNotNull(book().order(otherAsk));
        }

        @Test
        void fokHonoursSelfTradePrevention()
        {
            aliceBuys(SelfTradePrevention.CANCEL_MAKER, TimeInForce.FOK);
            assertEquals(Reason.FOK_NOT_FILLABLE, reports().get(1).reason(), "only 5 non-own available");
            sink.clear();
            aliceBuys(SelfTradePrevention.CANCEL_TAKER, TimeInForce.FOK);
            assertEquals(Reason.FOK_NOT_FILLABLE, reports().get(1).reason(), "own order blocks first");
            sink.clear();
            aliceBuys(SelfTradePrevention.NONE, TimeInForce.FOK);
            assertEquals(2, trades().size(), "self trade allowed, 10 available");
        }

        @Test
        void fokCancelMakerFillableWhenEnoughOtherLiquidity()
        {
            limit(CAROL, SELL, 100, 5);
            sink.clear();
            aliceBuys(SelfTradePrevention.CANCEL_MAKER, TimeInForce.FOK);
            assertEquals(2, trades().size());
            assertNull(book().order(ownAsk));
        }
    }

    @Nested
    class MarketDataAndSnapshots
    {
        @Test
        void bookSequenceIncrementsOnlyWhenTheBookChanges()
        {
            limit(ALICE, BUY, 100, 10);
            limit(ALICE, BUY, 101, 10);
            market(BOB, BUY, 1);                // no liquidity, no change
            cancel(BOB, GBPUSD, 999);           // rejected, no change
            limit(ALICE, BUY, 102, 10);
            assertEquals(List.of(1L, 2L, 3L),
                sink.ofType(BookUpdate.class).stream().map(BookUpdate::bookSequence).toList());
            assertEquals(3, book().bookSequence());
        }

        @Test
        void snapshotFullAndLimitedDepth()
        {
            limit(ALICE, BUY, 100, 10);
            limit(BOB, BUY, 100, 5);
            limit(ALICE, BUY, 99, 1);
            limit(ALICE, SELL, 103, 7);
            limit(ALICE, SELL, 104, 8);
            sink.clear();

            engine.onBookSnapshotRequest(77, 770, GBPUSD);
            final Snapshot full = sink.ofType(Snapshot.class).get(0);
            assertEquals(77, full.engineSequence());
            assertEquals(5, full.bookSequence());
            assertEquals(List.of(new Level(BUY, 100, 15, 2), new Level(BUY, 99, 1, 1)), full.bids());
            assertEquals(List.of(new Level(SELL, 103, 7, 1), new Level(SELL, 104, 8, 1)), full.asks());

            final RecordingEventSink depthSink = new RecordingEventSink();
            final MatchingEngine shallow = new MatchingEngine(
                List.of(new Instrument(GBPUSD, "GBPUSD", 1, 1, 1, 100, 0)), depthSink, 1, 1);
            for (int i = 0; i < 3; i++)
            {
                shallow.onNewOrder(i, 0, i + 1, i, ALICE, GBPUSD, BUY.code(), OrderType.LIMIT.code(),
                    TimeInForce.GTC.code(), (byte)0, false, 10 + i, 1);
            }
            shallow.onBookSnapshotRequest(9, 0, GBPUSD);
            final Snapshot limited = depthSink.ofType(Snapshot.class).get(0);
            assertEquals(List.of(new Level(BUY, 12, 1, 1)), limited.bids());
            assertTrue(limited.asks().isEmpty());
        }

        @Test
        void snapshotOfUnknownInstrumentIsIgnored()
        {
            engine.onBookSnapshotRequest(1, 1, 42);
            assertTrue(sink.events.isEmpty());
        }

        @Test
        void booksAreIndependentPerInstrument()
        {
            submit(EURUSD, ALICE, SELL, OrderType.LIMIT, TimeInForce.GTC, SelfTradePrevention.NONE, false, 100, 100);
            limit(BOB, BUY, 100, 10);
            assertTrue(trades().isEmpty());
            submit(EURUSD, BOB, BUY, OrderType.LIMIT, TimeInForce.GTC, SelfTradePrevention.NONE, false, 100, 100);
            assertEquals(((long)EURUSD << 40) | 1, trades().get(0).tradeId());
            assertEquals(1, engine.book(GBPUSD).orderCount());
            assertNull(engine.book(99));
        }
    }

    @Nested
    class Resources
    {
        @Test
        void ordersAreRecycledThroughThePool()
        {
            final int initial = engine.orderPool().available();
            for (int i = 0; i < 100; i++)
            {
                final long id = limit(ALICE, BUY, 100, 10);
                if (i % 2 == 0)
                {
                    cancel(ALICE, GBPUSD, id);
                }
                else
                {
                    limit(BOB, SELL, 100, 10);
                }
            }
            assertEquals(initial, engine.orderPool().available());
            assertEquals(0, book().orderCount());
            assertFalse(engine.orderPool().allocationsBeyondInitial() > 1);
        }

        @Test
        void constructorValidation()
        {
            final Instrument i = new Instrument(1, "A", 1, 1, 1, 10, 0);
            assertThrows(IllegalArgumentException.class, () -> new MatchingEngine(List.of(i, i), sink, 1, 0));
            assertThrows(IllegalArgumentException.class, () -> new MatchingEngine(List.of(i), sink, 1, -1));
            assertThrows(NullPointerException.class, () -> new MatchingEngine(List.of(i), null, 1, 0));
        }

        @Test
        void orderAccessorsAndToString()
        {
            final long id = submit(GBPUSD, ALICE, BUY, OrderType.LIMIT, TimeInForce.GTC,
                SelfTradePrevention.CANCEL_TAKER, false, 100, 10);
            final Order o = book().order(id);
            assertEquals(1000, o.clientOrderId());
            assertEquals(GBPUSD, o.instrumentId());
            assertEquals(OrderType.LIMIT, o.orderType());
            assertEquals(TimeInForce.GTC, o.timeInForce());
            assertEquals(SelfTradePrevention.CANCEL_TAKER, o.selfTradePrevention());
            assertEquals(0, o.timestamp());
            assertTrue(o.toString().contains("BUY 10/10 @ 100"));
            final PriceLevel level = book().bids().best();
            assertEquals(BUY, level.side());
            assertFalse(level.isEmpty());
            assertEquals(BUY, book().bids().side());
            assertEquals(SELL, book().asks().side());
            assertEquals(GBPUSD, book().instrument().id());
            assertEquals(0, book().levelQuantity(SELL, 100));
            assertEquals(0, book().levelOrderCount(SELL, 100));
        }
    }

    @Nested
    class Collar
    {
        final Instrument instrument = new Instrument(1, "X", 10, 1, 1, 100, 100); // 1%

        BookSide sideWithBest(final Side side, final long price)
        {
            final BookSide bookSide = new BookSide(side, 4);
            final PriceLevel level = new PriceLevel();
            level.init(side, price);
            bookSide.insert(level);
            return bookSide;
        }

        @ParameterizedTest
        @EnumSource(Side.class)
        void unboundedWithoutLiquidity(final Side side)
        {
            assertEquals(side == BUY ? Long.MAX_VALUE : 0,
                MatchingEngine.collarPrice(instrument, side, new BookSide(side.opposite(), 4)));
        }

        @Test
        void roundsInsideTheBand()
        {
            assertEquals(10_100, MatchingEngine.collarPrice(instrument, BUY, sideWithBest(SELL, 10_000)));
            assertEquals(10_110, MatchingEngine.collarPrice(instrument, BUY, sideWithBest(SELL, 10_015)));
            assertEquals(9_900, MatchingEngine.collarPrice(instrument, SELL, sideWithBest(BUY, 10_000)));
            assertEquals(9_920, MatchingEngine.collarPrice(instrument, SELL, sideWithBest(BUY, 10_015)));
        }

        @Test
        void clampsAtExtremes()
        {
            final Instrument full = new Instrument(1, "X", 1, 1, 1, 100, 10_000);
            assertEquals(0, MatchingEngine.collarPrice(full, SELL, sideWithBest(BUY, 50)));
            assertEquals(Long.MAX_VALUE, MatchingEngine.collarPrice(full, BUY,
                sideWithBest(SELL, Long.MAX_VALUE - 10)));
        }
    }
}
