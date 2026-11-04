package com.flashexchange.engine.matching;

import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.Liquidity;
import com.flashexchange.protocol.OrderStatus;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import org.agrona.collections.Int2ObjectHashMap;

import java.util.Collection;
import java.util.Objects;

/**
 * Price-time priority matching engine for a set of instruments. Single threaded and deterministic: the same
 * sequence of commands (including the engine sequence, order id and timestamp supplied by the caller) always
 * produces the same events.
 *
 * <h2>Rules</h2>
 * <ul>
 *   <li>Incoming orders match the best opposite level first; within a level the oldest order first (FIFO).</li>
 *   <li>Trades execute at the resting (maker) order's price.</li>
 *   <li>LIMIT + GTC: unfilled remainder rests on the book. LIMIT + IOC: remainder cancelled.
 *       FOK (limit or market): filled completely or cancelled without trading.</li>
 *   <li>MARKET: never rests. Matching is bounded by a protection price ("collar") of
 *       {@link Instrument#collarBps()} around the best opposite price at entry. GTC on a market order is treated
 *       as IOC.</li>
 *   <li>Post-only limit orders that would trade on entry are rejected.</li>
 *   <li>Self-trade prevention is applied by the incoming order's mode.</li>
 * </ul>
 *
 * <h2>Events per accepted order</h2>
 * NEW ack, then for each fill: trade, maker report, taker report; then a CANCELED report for any remainder that does
 * not rest; finally one book update if any level changed. Invalid orders get a single REJECTED report.
 */
public final class MatchingEngine
{
    private static final long BPS_DIVISOR = 10_000L;

    private final Int2ObjectHashMap<OrderBook> books = new Int2ObjectHashMap<>();
    private final ObjectPool<Order> orderPool;
    private final ObjectPool<PriceLevel> levelPool;
    private final EngineEventSink sink;
    private final int snapshotDepth;

    /**
     * Create an engine.
     *
     * @param instruments   instruments handled by this engine.
     * @param sink          receiving the engine output.
     * @param orderPoolSize number of pre-allocated orders.
     * @param snapshotDepth maximum levels per side in snapshots (0 = full depth).
     */
    public MatchingEngine(
        final Collection<Instrument> instruments,
        final EngineEventSink sink,
        final int orderPoolSize,
        final int snapshotDepth)
    {
        this.sink = Objects.requireNonNull(sink, "sink");
        if (snapshotDepth < 0)
        {
            throw new IllegalArgumentException("snapshotDepth must be >= 0");
        }
        this.snapshotDepth = snapshotDepth;
        this.orderPool = new ObjectPool<>(orderPoolSize, Order::new, Order::reset);
        this.levelPool = new ObjectPool<>(Math.max(16, orderPoolSize / 4), PriceLevel::new, PriceLevel::reset);
        for (final Instrument instrument : instruments)
        {
            if (books.containsKey(instrument.id()))
            {
                throw new IllegalArgumentException("duplicate instrument id: " + instrument.id());
            }
            books.put(instrument.id(), new OrderBook(instrument, 256));
        }
    }

    /**
     * Book of an instrument.
     *
     * @param instrumentId instrument.
     * @return the book or {@code null} if the instrument is not handled here.
     */
    public OrderBook book(final int instrumentId)
    {
        return books.get(instrumentId);
    }

    /**
     * Pool of orders (exposed for monitoring and tests).
     *
     * @return the order pool.
     */
    public ObjectPool<Order> orderPool()
    {
        return orderPool;
    }

    /**
     * Process a new order.
     *
     * @param engineSequence      sequence of the command.
     * @param timestamp           engine timestamp of the command.
     * @param orderId             id assigned to the order (unique).
     * @param clientOrderId       client order id.
     * @param userId              user.
     * @param instrumentId        instrument.
     * @param sideCode            side wire code.
     * @param orderTypeCode       order type wire code.
     * @param timeInForceCode     time in force wire code.
     * @param stpCode             self-trade prevention wire code.
     * @param postOnly            post-only flag.
     * @param price               limit price (0 for market).
     * @param quantity            quantity.
     */
    public void onNewOrder(
        final long engineSequence,
        final long timestamp,
        final long orderId,
        final long clientOrderId,
        final long userId,
        final int instrumentId,
        final byte sideCode,
        final byte orderTypeCode,
        final byte timeInForceCode,
        final byte stpCode,
        final boolean postOnly,
        final long price,
        final long quantity)
    {
        final OrderBook book = books.get(instrumentId);
        final Side side = Side.get(sideCode);
        final OrderType orderType = OrderType.get(orderTypeCode);
        final TimeInForce timeInForce = TimeInForce.get(timeInForceCode);
        final SelfTradePrevention stp = SelfTradePrevention.get(stpCode);

        final Reason reason = validate(book, side, orderType, timeInForce, stp, postOnly, price, quantity);
        if (reason != Reason.NONE)
        {
            sink.onOrderRejected(
                engineSequence, timestamp, orderId, clientOrderId, userId, instrumentId,
                sideCode, orderTypeCode, timeInForceCode, price, quantity, reason);
            return;
        }

        final Order taker = orderPool.acquire();
        taker.orderId = orderId;
        taker.clientOrderId = clientOrderId;
        taker.userId = userId;
        taker.instrumentId = instrumentId;
        taker.side = side;
        taker.orderType = orderType;
        taker.timeInForce = orderType == OrderType.MARKET && timeInForce == TimeInForce.GTC ?
            TimeInForce.IOC : timeInForce;
        taker.selfTradePrevention = stp;
        taker.price = orderType == OrderType.MARKET ? 0 : price;
        taker.quantity = quantity;
        taker.timestamp = timestamp;

        sink.onExecutionReport(
            engineSequence, timestamp, taker, ExecType.NEW, OrderStatus.NEW, 0, 0, 0, Liquidity.NONE, Reason.NONE);

        final BookSide opposite = book.side(side.opposite());
        final long limit = orderType == OrderType.LIMIT ? price : collarPrice(book.instrument(), side, opposite);

        if (taker.timeInForce == TimeInForce.FOK && fillableQuantity(opposite, limit, taker) < quantity)
        {
            cancelTaker(engineSequence, timestamp, taker, Reason.FOK_NOT_FILLABLE);
        }
        else
        {
            final Reason stopReason = match(engineSequence, timestamp, book, opposite, taker, limit);
            if (stopReason != Reason.NONE)
            {
                cancelTaker(engineSequence, timestamp, taker, stopReason);
            }
            else if (taker.remainingQuantity() == 0)
            {
                orderPool.release(taker);
            }
            else if (orderType == OrderType.LIMIT && taker.timeInForce == TimeInForce.GTC)
            {
                book.addResting(taker, levelPool);
            }
            else
            {
                cancelTaker(engineSequence, timestamp, taker, remainderReason(taker, opposite));
            }
        }

        publishBookUpdate(engineSequence, timestamp, book);
    }

    /**
     * Process a cancel request.
     *
     * @param engineSequence sequence of the command.
     * @param timestamp      engine timestamp.
     * @param clientOrderId  client id of the cancel request.
     * @param userId         requesting user.
     * @param instrumentId   instrument of the order.
     * @param orderId        order to cancel.
     */
    public void onCancelOrder(
        final long engineSequence,
        final long timestamp,
        final long clientOrderId,
        final long userId,
        final int instrumentId,
        final long orderId)
    {
        final OrderBook book = books.get(instrumentId);
        if (book == null)
        {
            sink.onCancelRejected(
                engineSequence, timestamp, orderId, clientOrderId, userId, instrumentId, Reason.UNKNOWN_INSTRUMENT);
            return;
        }

        final Order order = book.order(orderId);
        if (order == null)
        {
            sink.onCancelRejected(
                engineSequence, timestamp, orderId, clientOrderId, userId, instrumentId, Reason.UNKNOWN_ORDER);
            return;
        }

        if (order.userId != userId)
        {
            sink.onCancelRejected(
                engineSequence, timestamp, orderId, clientOrderId, userId, instrumentId, Reason.NOT_ORDER_OWNER);
            return;
        }

        cancelResting(engineSequence, timestamp, book, order, Reason.USER_CANCEL);
        publishBookUpdate(engineSequence, timestamp, book);
    }

    /**
     * Emit a snapshot of a book. Unknown instruments are ignored.
     *
     * @param engineSequence sequence of the command.
     * @param timestamp      engine timestamp.
     * @param instrumentId   instrument.
     */
    public void onBookSnapshotRequest(final long engineSequence, final long timestamp, final int instrumentId)
    {
        final OrderBook book = books.get(instrumentId);
        if (book != null)
        {
            sink.onBookSnapshot(engineSequence, timestamp, book, snapshotDepth);
        }
    }

    private static Reason validate(
        final OrderBook book,
        final Side side,
        final OrderType orderType,
        final TimeInForce timeInForce,
        final SelfTradePrevention stp,
        final boolean postOnly,
        final long price,
        final long quantity)
    {
        if (book == null)
        {
            return Reason.UNKNOWN_INSTRUMENT;
        }
        if (side == null)
        {
            return Reason.INVALID_SIDE;
        }
        if (orderType == null)
        {
            return Reason.INVALID_ORDER_TYPE;
        }
        if (timeInForce == null)
        {
            return Reason.INVALID_TIME_IN_FORCE;
        }
        if (stp == null)
        {
            return Reason.INVALID_SELF_TRADE_PREVENTION;
        }

        final Instrument instrument = book.instrument();
        if (quantity <= 0 || quantity % instrument.lotSize() != 0)
        {
            return Reason.INVALID_QUANTITY;
        }
        if (quantity < instrument.minQuantity())
        {
            return Reason.BELOW_MIN_QUANTITY;
        }
        if (quantity > instrument.maxQuantity())
        {
            return Reason.ABOVE_MAX_QUANTITY;
        }

        if (orderType == OrderType.MARKET)
        {
            if (price != 0)
            {
                return Reason.INVALID_PRICE;
            }
            if (postOnly)
            {
                return Reason.POST_ONLY_NOT_ALLOWED;
            }
        }
        else
        {
            if (price <= 0 || price % instrument.tickSize() != 0)
            {
                return Reason.INVALID_PRICE;
            }
            if (postOnly)
            {
                final BookSide opposite = book.side(side.opposite());
                final PriceLevel best = opposite.best();
                if (best != null && opposite.crosses(best.price, price))
                {
                    return Reason.POST_ONLY_WOULD_TAKE;
                }
            }
        }

        return Reason.NONE;
    }

    /**
     * Protection limit for a market order: best opposite price moved by the collar towards the taker's
     * disadvantage, rounded to the tick in the direction that keeps the collar inside the configured band.
     */
    static long collarPrice(final Instrument instrument, final Side side, final BookSide opposite)
    {
        final PriceLevel best = opposite.best();
        if (instrument.collarBps() == 0 || best == null)
        {
            return side == Side.BUY ? Long.MAX_VALUE : 0;
        }

        final long reference = best.price;
        final long bps = instrument.collarBps();
        final long delta = (reference / BPS_DIVISOR) * bps + ((reference % BPS_DIVISOR) * bps) / BPS_DIVISOR;
        final long tick = instrument.tickSize();
        if (side == Side.BUY)
        {
            final long raw = reference > Long.MAX_VALUE - delta ? Long.MAX_VALUE : reference + delta;
            return (raw / tick) * tick;
        }
        else
        {
            final long raw = reference - delta;
            return raw <= 0 ? 0 : ((raw + tick - 1) / tick) * tick;
        }
    }

    /**
     * Quantity the taker could fill right now, honouring its limit and self-trade prevention mode.
     */
    private static long fillableQuantity(final BookSide opposite, final long limit, final Order taker)
    {
        final SelfTradePrevention stp = taker.selfTradePrevention;
        long available = 0;
        for (int depth = 0, levels = opposite.levelCount(); depth < levels; depth++)
        {
            final PriceLevel level = opposite.levelAt(depth);
            if (!opposite.crosses(level.price, limit))
            {
                break;
            }
            for (Order maker = level.head; maker != null; maker = maker.next)
            {
                if (maker.userId == taker.userId && stp != SelfTradePrevention.NONE)
                {
                    if (stp == SelfTradePrevention.CANCEL_MAKER)
                    {
                        continue;
                    }
                    return available;
                }
                available += maker.remainingQuantity();
                if (available >= taker.remainingQuantity())
                {
                    return available;
                }
            }
        }
        return available;
    }

    /**
     * Match the taker against the opposite side.
     *
     * @return {@link Reason#SELF_TRADE_PREVENTION} if self-trade prevention stopped the taker, else NONE.
     */
    private Reason match(
        final long engineSequence,
        final long timestamp,
        final OrderBook book,
        final BookSide opposite,
        final Order taker,
        final long limit)
    {
        while (taker.remainingQuantity() > 0)
        {
            final PriceLevel level = opposite.best();
            if (level == null || !opposite.crosses(level.price, limit))
            {
                break;
            }

            final Order maker = level.head;
            if (maker.userId == taker.userId && taker.selfTradePrevention != SelfTradePrevention.NONE)
            {
                switch (taker.selfTradePrevention)
                {
                    case CANCEL_MAKER:
                        cancelResting(engineSequence, timestamp, book, maker, Reason.SELF_TRADE_PREVENTION);
                        continue;

                    case CANCEL_BOTH:
                        cancelResting(engineSequence, timestamp, book, maker, Reason.SELF_TRADE_PREVENTION);
                        return Reason.SELF_TRADE_PREVENTION;

                    default: // CANCEL_TAKER
                        return Reason.SELF_TRADE_PREVENTION;
                }
            }

            final long fillQuantity = Math.min(taker.remainingQuantity(), maker.remainingQuantity());
            final long fillPrice = level.price;
            final long tradeId = book.nextTradeId();

            maker.filledQuantity += fillQuantity;
            taker.filledQuantity += fillQuantity;
            level.reduce(fillQuantity);
            book.touch(level.side, level.price);

            final Order buy = taker.side == Side.BUY ? taker : maker;
            final Order sell = taker.side == Side.BUY ? maker : taker;
            sink.onTrade(
                engineSequence, timestamp, tradeId, book.instrument().id(), fillPrice, fillQuantity, buy, sell,
                taker.side);
            sink.onExecutionReport(
                engineSequence, timestamp, maker, ExecType.TRADE, fillStatus(maker), fillPrice, fillQuantity,
                tradeId, Liquidity.MAKER, Reason.NONE);
            sink.onExecutionReport(
                engineSequence, timestamp, taker, ExecType.TRADE, fillStatus(taker), fillPrice, fillQuantity,
                tradeId, Liquidity.TAKER, Reason.NONE);

            if (maker.remainingQuantity() == 0)
            {
                book.removeResting(maker, levelPool);
                orderPool.release(maker);
            }
        }

        return Reason.NONE;
    }

    private static OrderStatus fillStatus(final Order order)
    {
        return order.remainingQuantity() == 0 ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
    }

    private static Reason remainderReason(final Order taker, final BookSide opposite)
    {
        if (taker.orderType == OrderType.MARKET)
        {
            return opposite.isEmpty() ? Reason.NO_LIQUIDITY : Reason.PRICE_COLLAR;
        }
        return Reason.IOC_REMAINDER;
    }

    private void cancelTaker(
        final long engineSequence, final long timestamp, final Order taker, final Reason reason)
    {
        sink.onExecutionReport(
            engineSequence, timestamp, taker, ExecType.CANCELED, OrderStatus.CANCELED, 0, 0, 0, Liquidity.NONE,
            reason);
        orderPool.release(taker);
    }

    private void cancelResting(
        final long engineSequence,
        final long timestamp,
        final OrderBook book,
        final Order order,
        final Reason reason)
    {
        book.removeResting(order, levelPool);
        sink.onExecutionReport(
            engineSequence, timestamp, order, ExecType.CANCELED, OrderStatus.CANCELED, 0, 0, 0, Liquidity.NONE,
            reason);
        orderPool.release(order);
    }

    private void publishBookUpdate(final long engineSequence, final long timestamp, final OrderBook book)
    {
        if (book.changedLevelCount() > 0)
        {
            book.nextBookSequence();
            sink.onBookUpdate(engineSequence, timestamp, book);
            book.clearChanges();
        }
    }
}
