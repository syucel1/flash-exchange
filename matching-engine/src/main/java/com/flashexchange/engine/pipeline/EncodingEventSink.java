package com.flashexchange.engine.pipeline;

import com.flashexchange.engine.matching.BookSide;
import com.flashexchange.engine.matching.EngineEventSink;
import com.flashexchange.engine.matching.Order;
import com.flashexchange.engine.matching.OrderBook;
import com.flashexchange.engine.matching.PriceLevel;
import com.flashexchange.protocol.BookSnapshotFlyweight;
import com.flashexchange.protocol.BookUpdateFlyweight;
import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.ExecutionReportFlyweight;
import com.flashexchange.protocol.Liquidity;
import com.flashexchange.protocol.OrderStatus;
import com.flashexchange.protocol.PublicTradeFlyweight;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TradeExecutionFlyweight;

/**
 * {@link EngineEventSink} that encodes engine output into the {@link MessageBatch}es of the ring slot currently
 * being processed:
 * <ul>
 *   <li>execution output (stream 2001): {@code ExecutionReport}, {@code TradeExecution};</li>
 *   <li>market data output (stream 3001): {@code PublicTrade}, {@code BookUpdate}, {@code BookSnapshot}.</li>
 * </ul>
 * One instance per matcher thread; not thread safe.
 */
public final class EncodingEventSink implements EngineEventSink
{
    private final ExecutionReportFlyweight executionReport = new ExecutionReportFlyweight();
    private final TradeExecutionFlyweight tradeExecution = new TradeExecutionFlyweight();
    private final PublicTradeFlyweight publicTrade = new PublicTradeFlyweight();
    private final BookUpdateFlyweight bookUpdate = new BookUpdateFlyweight();
    private final BookSnapshotFlyweight bookSnapshot = new BookSnapshotFlyweight();

    private MessageBatch executionOutput;
    private MessageBatch marketDataOutput;

    /**
     * Direct subsequent output to the given batches.
     *
     * @param executionOutput  for private events.
     * @param marketDataOutput for market data.
     */
    public void bind(final MessageBatch executionOutput, final MessageBatch marketDataOutput)
    {
        this.executionOutput = executionOutput;
        this.marketDataOutput = marketDataOutput;
    }

    @Override
    public void onExecutionReport(
        final long engineSequence,
        final long timestamp,
        final Order order,
        final ExecType execType,
        final OrderStatus orderStatus,
        final long lastPrice,
        final long lastQuantity,
        final long tradeId,
        final Liquidity liquidity,
        final Reason reason)
    {
        final boolean open = orderStatus == OrderStatus.NEW || orderStatus == OrderStatus.PARTIALLY_FILLED;
        executionReport.wrapForEncode(executionOutput.buffer(), executionOutput.limit())
            .engineSequence(engineSequence)
            .timestamp(timestamp)
            .orderId(order.orderId())
            .clientOrderId(order.clientOrderId())
            .userId(order.userId())
            .price(order.price())
            .quantity(order.quantity())
            .lastPrice(lastPrice)
            .lastQuantity(lastQuantity)
            .cumulativeQuantity(order.filledQuantity())
            .leavesQuantity(open ? order.remainingQuantity() : 0)
            .tradeId(tradeId)
            .instrumentId(order.instrumentId())
            .execType(execType)
            .orderStatus(orderStatus)
            .side(order.side())
            .orderType(order.orderType())
            .timeInForce(order.timeInForce())
            .liquidity(liquidity)
            .reason(reason);
        executionOutput.commit(ExecutionReportFlyweight.ENCODED_LENGTH);
    }

    @Override
    public void onOrderRejected(
        final long engineSequence,
        final long timestamp,
        final long orderId,
        final long clientOrderId,
        final long userId,
        final int instrumentId,
        final byte sideCode,
        final byte orderTypeCode,
        final byte timeInForceCode,
        final long price,
        final long quantity,
        final Reason reason)
    {
        executionReport.wrapForEncode(executionOutput.buffer(), executionOutput.limit())
            .engineSequence(engineSequence)
            .timestamp(timestamp)
            .orderId(orderId)
            .clientOrderId(clientOrderId)
            .userId(userId)
            .price(price)
            .quantity(quantity)
            .lastPrice(0)
            .lastQuantity(0)
            .cumulativeQuantity(0)
            .leavesQuantity(0)
            .tradeId(0)
            .instrumentId(instrumentId)
            .execType(ExecType.REJECTED)
            .orderStatus(OrderStatus.REJECTED)
            .sideCode(sideCode)
            .orderTypeCode(orderTypeCode)
            .timeInForceCode(timeInForceCode)
            .liquidity(Liquidity.NONE)
            .reason(reason);
        executionOutput.commit(ExecutionReportFlyweight.ENCODED_LENGTH);
    }

    @Override
    public void onCancelRejected(
        final long engineSequence,
        final long timestamp,
        final long orderId,
        final long clientOrderId,
        final long userId,
        final int instrumentId,
        final Reason reason)
    {
        executionReport.wrapForEncode(executionOutput.buffer(), executionOutput.limit())
            .engineSequence(engineSequence)
            .timestamp(timestamp)
            .orderId(orderId)
            .clientOrderId(clientOrderId)
            .userId(userId)
            .price(0)
            .quantity(0)
            .lastPrice(0)
            .lastQuantity(0)
            .cumulativeQuantity(0)
            .leavesQuantity(0)
            .tradeId(0)
            .instrumentId(instrumentId)
            .execType(ExecType.CANCEL_REJECTED)
            .orderStatus(OrderStatus.REJECTED)
            .sideCode((byte)0)
            .orderTypeCode((byte)0)
            .timeInForceCode((byte)0)
            .liquidity(Liquidity.NONE)
            .reason(reason);
        executionOutput.commit(ExecutionReportFlyweight.ENCODED_LENGTH);
    }

    @Override
    public void onTrade(
        final long engineSequence,
        final long timestamp,
        final long tradeId,
        final int instrumentId,
        final long price,
        final long quantity,
        final Order buyOrder,
        final Order sellOrder,
        final Side aggressorSide)
    {
        tradeExecution.wrapForEncode(executionOutput.buffer(), executionOutput.limit())
            .engineSequence(engineSequence)
            .timestamp(timestamp)
            .tradeId(tradeId)
            .price(price)
            .quantity(quantity)
            .buyOrderId(buyOrder.orderId())
            .sellOrderId(sellOrder.orderId())
            .buyUserId(buyOrder.userId())
            .sellUserId(sellOrder.userId())
            .buyClientOrderId(buyOrder.clientOrderId())
            .sellClientOrderId(sellOrder.clientOrderId())
            .instrumentId(instrumentId)
            .aggressorSide(aggressorSide);
        executionOutput.commit(TradeExecutionFlyweight.ENCODED_LENGTH);

        publicTrade.wrapForEncode(marketDataOutput.buffer(), marketDataOutput.limit())
            .engineSequence(engineSequence)
            .timestamp(timestamp)
            .tradeId(tradeId)
            .price(price)
            .quantity(quantity)
            .instrumentId(instrumentId)
            .aggressorSide(aggressorSide);
        marketDataOutput.commit(PublicTradeFlyweight.ENCODED_LENGTH);
    }

    @Override
    public void onBookUpdate(final long engineSequence, final long timestamp, final OrderBook book)
    {
        final int count = book.changedLevelCount();
        bookUpdate.wrapForEncode(marketDataOutput.buffer(), marketDataOutput.limit())
            .engineSequence(engineSequence)
            .timestamp(timestamp)
            .bookSequence(book.bookSequence())
            .instrumentId(book.instrument().id())
            .levelCount(count);
        for (int i = 0; i < count; i++)
        {
            final Side side = book.changedSide(i);
            final long price = book.changedPrice(i);
            bookUpdate.putLevel(i, side, price, book.levelQuantity(side, price), book.levelOrderCount(side, price));
        }
        marketDataOutput.commit(BookUpdateFlyweight.encodedLength(count));
    }

    @Override
    public void onBookSnapshot(
        final long engineSequence, final long timestamp, final OrderBook book, final int depth)
    {
        final BookSide bids = book.bids();
        final BookSide asks = book.asks();
        final int bidCount = depth == 0 ? bids.levelCount() : Math.min(depth, bids.levelCount());
        final int askCount = depth == 0 ? asks.levelCount() : Math.min(depth, asks.levelCount());

        bookSnapshot.wrapForEncode(marketDataOutput.buffer(), marketDataOutput.limit())
            .engineSequence(engineSequence)
            .timestamp(timestamp)
            .bookSequence(book.bookSequence())
            .instrumentId(book.instrument().id())
            .bidCount(bidCount);
        for (int i = 0; i < bidCount; i++)
        {
            final PriceLevel level = bids.levelAt(i);
            bookSnapshot.putBid(i, level.price(), level.totalQuantity(), level.orderCount());
        }
        bookSnapshot.askCount(askCount);
        for (int i = 0; i < askCount; i++)
        {
            final PriceLevel level = asks.levelAt(i);
            bookSnapshot.putAsk(i, level.price(), level.totalQuantity(), level.orderCount());
        }
        marketDataOutput.commit(BookSnapshotFlyweight.encodedLength(bidCount, askCount));
    }
}
