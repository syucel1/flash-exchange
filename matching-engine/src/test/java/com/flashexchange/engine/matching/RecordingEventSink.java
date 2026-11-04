package com.flashexchange.engine.matching;

import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.Liquidity;
import com.flashexchange.protocol.OrderStatus;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.Side;

import java.util.ArrayList;
import java.util.List;

/**
 * Test sink that copies every callback into immutable records.
 */
public final class RecordingEventSink implements EngineEventSink
{
    public sealed interface Event permits Report, Reject, CancelReject, Trade, BookUpdate, Snapshot
    {
    }

    public record Report(
        long engineSequence, long timestamp, long orderId, long clientOrderId, long userId, int instrumentId,
        Side side, long price, long quantity, ExecType execType, OrderStatus status, long lastPrice,
        long lastQuantity, long cumulativeQuantity, long remainingQuantity, long tradeId, Liquidity liquidity,
        Reason reason) implements Event
    {
    }

    public record Reject(
        long engineSequence, long orderId, long clientOrderId, long userId, int instrumentId, byte side,
        byte orderType, byte timeInForce, long price, long quantity, Reason reason) implements Event
    {
    }

    public record CancelReject(
        long engineSequence, long orderId, long clientOrderId, long userId, int instrumentId, Reason reason)
        implements Event
    {
    }

    public record Trade(
        long engineSequence, long tradeId, int instrumentId, long price, long quantity, long buyOrderId,
        long sellOrderId, long buyUserId, long sellUserId, Side aggressor) implements Event
    {
    }

    public record Level(Side side, long price, long quantity, int orderCount)
    {
    }

    public record BookUpdate(long engineSequence, int instrumentId, long bookSequence, List<Level> levels)
        implements Event
    {
    }

    public record Snapshot(
        long engineSequence, int instrumentId, long bookSequence, List<Level> bids, List<Level> asks)
        implements Event
    {
    }

    public final List<Event> events = new ArrayList<>();

    @Override
    public void onExecutionReport(
        final long engineSequence, final long timestamp, final Order order, final ExecType execType,
        final OrderStatus orderStatus, final long lastPrice, final long lastQuantity, final long tradeId,
        final Liquidity liquidity, final Reason reason)
    {
        events.add(new Report(engineSequence, timestamp, order.orderId(), order.clientOrderId(), order.userId(),
            order.instrumentId(), order.side(), order.price(), order.quantity(), execType, orderStatus, lastPrice,
            lastQuantity, order.filledQuantity(), order.remainingQuantity(), tradeId, liquidity, reason));
    }

    @Override
    public void onOrderRejected(
        final long engineSequence, final long timestamp, final long orderId, final long clientOrderId,
        final long userId, final int instrumentId, final byte sideCode, final byte orderTypeCode,
        final byte timeInForceCode, final long price, final long quantity, final Reason reason)
    {
        events.add(new Reject(engineSequence, orderId, clientOrderId, userId, instrumentId, sideCode, orderTypeCode,
            timeInForceCode, price, quantity, reason));
    }

    @Override
    public void onCancelRejected(
        final long engineSequence, final long timestamp, final long orderId, final long clientOrderId,
        final long userId, final int instrumentId, final Reason reason)
    {
        events.add(new CancelReject(engineSequence, orderId, clientOrderId, userId, instrumentId, reason));
    }

    @Override
    public void onTrade(
        final long engineSequence, final long timestamp, final long tradeId, final int instrumentId,
        final long price, final long quantity, final Order buyOrder, final Order sellOrder, final Side aggressorSide)
    {
        events.add(new Trade(engineSequence, tradeId, instrumentId, price, quantity, buyOrder.orderId(),
            sellOrder.orderId(), buyOrder.userId(), sellOrder.userId(), aggressorSide));
    }

    @Override
    public void onBookUpdate(final long engineSequence, final long timestamp, final OrderBook book)
    {
        final List<Level> levels = new ArrayList<>();
        for (int i = 0; i < book.changedLevelCount(); i++)
        {
            final Side side = book.changedSide(i);
            final long price = book.changedPrice(i);
            levels.add(new Level(side, price, book.levelQuantity(side, price), book.levelOrderCount(side, price)));
        }
        events.add(new BookUpdate(engineSequence, book.instrument().id(), book.bookSequence(), levels));
    }

    @Override
    public void onBookSnapshot(
        final long engineSequence, final long timestamp, final OrderBook book, final int depth)
    {
        events.add(new Snapshot(engineSequence, book.instrument().id(), book.bookSequence(),
            levels(book.bids(), depth), levels(book.asks(), depth)));
    }

    private static List<Level> levels(final BookSide side, final int depth)
    {
        final List<Level> result = new ArrayList<>();
        final int count = depth == 0 ? side.levelCount() : Math.min(depth, side.levelCount());
        for (int i = 0; i < count; i++)
        {
            final PriceLevel level = side.levelAt(i);
            result.add(new Level(level.side(), level.price(), level.totalQuantity(), level.orderCount()));
        }
        return result;
    }

    public <T extends Event> List<T> ofType(final Class<T> type)
    {
        final List<T> result = new ArrayList<>();
        for (final Event e : events)
        {
            if (type.isInstance(e))
            {
                result.add(type.cast(e));
            }
        }
        return result;
    }

    public void clear()
    {
        events.clear();
    }
}
