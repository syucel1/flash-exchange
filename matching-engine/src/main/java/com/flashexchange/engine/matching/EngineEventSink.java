package com.flashexchange.engine.matching;

import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.Liquidity;
import com.flashexchange.protocol.OrderStatus;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.Side;

/**
 * Receives everything the {@link MatchingEngine} produces while processing one command, in the order it happens.
 * <p>
 * Callbacks are invoked on the matching thread; {@link Order} and {@link OrderBook} arguments are live, pooled
 * objects that must only be read during the callback.
 */
public interface EngineEventSink
{
    /**
     * Execution report for an accepted order: NEW ack, fill, or cancel.
     *
     * @param engineSequence of the command.
     * @param timestamp      of the command.
     * @param order          the order, after the event has been applied.
     * @param execType       what happened.
     * @param orderStatus    order status after the event.
     * @param lastPrice      fill price (TRADE only, else 0).
     * @param lastQuantity   fill quantity (TRADE only, else 0).
     * @param tradeId        trade id (TRADE only, else 0).
     * @param liquidity      maker/taker indicator.
     * @param reason         cancel reason (CANCELED only, else NONE).
     */
    void onExecutionReport(
        long engineSequence,
        long timestamp,
        Order order,
        ExecType execType,
        OrderStatus orderStatus,
        long lastPrice,
        long lastQuantity,
        long tradeId,
        Liquidity liquidity,
        Reason reason);

    /**
     * An order failed validation. Raw wire codes are passed through because they may be invalid.
     *
     * @param engineSequence  of the command.
     * @param timestamp       of the command.
     * @param orderId         id assigned to the rejected order.
     * @param clientOrderId   client order id.
     * @param userId          user.
     * @param instrumentId    instrument as sent.
     * @param sideCode        side code as sent.
     * @param orderTypeCode   order type code as sent.
     * @param timeInForceCode time in force code as sent.
     * @param price           price as sent.
     * @param quantity        quantity as sent.
     * @param reason          why it was rejected.
     */
    void onOrderRejected(
        long engineSequence,
        long timestamp,
        long orderId,
        long clientOrderId,
        long userId,
        int instrumentId,
        byte sideCode,
        byte orderTypeCode,
        byte timeInForceCode,
        long price,
        long quantity,
        Reason reason);

    /**
     * A cancel request was rejected.
     *
     * @param engineSequence of the command.
     * @param timestamp      of the command.
     * @param orderId        order the cancel referred to.
     * @param clientOrderId  client id of the cancel request.
     * @param userId         requesting user.
     * @param instrumentId   instrument as sent.
     * @param reason         why it was rejected.
     */
    void onCancelRejected(
        long engineSequence,
        long timestamp,
        long orderId,
        long clientOrderId,
        long userId,
        int instrumentId,
        Reason reason);

    /**
     * A trade between a buy and a sell order. Invoked before the execution reports of the two orders.
     *
     * @param engineSequence of the command.
     * @param timestamp      of the command.
     * @param tradeId        unique trade id.
     * @param instrumentId   instrument.
     * @param price          execution price (maker price).
     * @param quantity       executed quantity.
     * @param buyOrder       buy side order (after the fill).
     * @param sellOrder      sell side order (after the fill).
     * @param aggressorSide  side of the taker.
     */
    void onTrade(
        long engineSequence,
        long timestamp,
        long tradeId,
        int instrumentId,
        long price,
        long quantity,
        Order buyOrder,
        Order sellOrder,
        Side aggressorSide);

    /**
     * The book changed; {@link OrderBook#changedLevelCount()} and friends describe the changed levels and
     * {@link OrderBook#bookSequence()} is the new book sequence. Invoked at most once per command, last.
     *
     * @param engineSequence of the command.
     * @param timestamp      of the command.
     * @param book           the book.
     */
    void onBookUpdate(long engineSequence, long timestamp, OrderBook book);

    /**
     * A snapshot of the book was requested.
     *
     * @param engineSequence of the command.
     * @param timestamp      of the command.
     * @param book           the book.
     * @param depth          maximum levels per side (0 = all).
     */
    void onBookSnapshot(long engineSequence, long timestamp, OrderBook book, int depth);
}
