package com.flashexchange.engine.pipeline;

/**
 * Pre-allocated Disruptor ring slot. The ingress thread copies a decoded command into the slot and stamps it with
 * the engine sequence and timestamp; the owning matcher shard processes it and encodes its output into
 * {@link #executionOutput()} and {@link #marketDataOutput()}; the publisher sends the output and clears the slot.
 */
public final class OrderCommand
{
    CommandType type;
    long engineSequence;
    long timestamp;
    long orderId;
    long clientOrderId;
    long userId;
    long price;
    long quantity;
    int instrumentId;
    byte side;
    byte orderType;
    byte timeInForce;
    byte selfTradePrevention;
    boolean postOnly;

    private final MessageBatch executionOutput = new MessageBatch(1024);
    private final MessageBatch marketDataOutput = new MessageBatch(1024);

    /**
     * Populate the slot with a new order command.
     *
     * @param clientOrderId       client order id.
     * @param userId              user.
     * @param instrumentId        instrument.
     * @param side                side wire code.
     * @param orderType           order type wire code.
     * @param timeInForce         time in force wire code.
     * @param selfTradePrevention self-trade prevention wire code.
     * @param postOnly            post-only flag.
     * @param price               limit price.
     * @param quantity            quantity.
     * @return this for fluent use.
     */
    public OrderCommand newOrder(
        final long clientOrderId,
        final long userId,
        final int instrumentId,
        final byte side,
        final byte orderType,
        final byte timeInForce,
        final byte selfTradePrevention,
        final boolean postOnly,
        final long price,
        final long quantity)
    {
        this.type = CommandType.NEW_ORDER;
        this.clientOrderId = clientOrderId;
        this.userId = userId;
        this.instrumentId = instrumentId;
        this.side = side;
        this.orderType = orderType;
        this.timeInForce = timeInForce;
        this.selfTradePrevention = selfTradePrevention;
        this.postOnly = postOnly;
        this.price = price;
        this.quantity = quantity;
        this.orderId = 0;
        return this;
    }

    /**
     * Populate the slot with a cancel command.
     *
     * @param clientOrderId client id of the cancel request.
     * @param userId        requesting user.
     * @param instrumentId  instrument.
     * @param orderId       order to cancel.
     * @return this for fluent use.
     */
    public OrderCommand cancelOrder(
        final long clientOrderId, final long userId, final int instrumentId, final long orderId)
    {
        this.type = CommandType.CANCEL_ORDER;
        this.clientOrderId = clientOrderId;
        this.userId = userId;
        this.instrumentId = instrumentId;
        this.orderId = orderId;
        return this;
    }

    /**
     * Populate the slot with a snapshot command for one instrument.
     *
     * @param instrumentId instrument.
     * @return this for fluent use.
     */
    public OrderCommand bookSnapshot(final int instrumentId)
    {
        this.type = CommandType.BOOK_SNAPSHOT;
        this.instrumentId = instrumentId;
        return this;
    }

    /**
     * Stamp the slot with the engine sequence and timestamp. New orders receive {@code orderId = sequence + 1}, which
     * makes order ids unique, monotonic and deterministic.
     *
     * @param engineSequence ring sequence.
     * @param timestamp      engine timestamp in epoch nanoseconds.
     * @return this for fluent use.
     */
    public OrderCommand stamp(final long engineSequence, final long timestamp)
    {
        this.engineSequence = engineSequence;
        this.timestamp = timestamp;
        if (type == CommandType.NEW_ORDER)
        {
            this.orderId = engineSequence + 1;
        }
        return this;
    }

    /**
     * Command type.
     *
     * @return type.
     */
    public CommandType type()
    {
        return type;
    }

    /**
     * Engine sequence.
     *
     * @return sequence.
     */
    public long engineSequence()
    {
        return engineSequence;
    }

    /**
     * Engine timestamp.
     *
     * @return epoch nanoseconds.
     */
    public long timestamp()
    {
        return timestamp;
    }

    /**
     * Order id (assigned for new orders, target for cancels).
     *
     * @return order id.
     */
    public long orderId()
    {
        return orderId;
    }

    /**
     * Instrument.
     *
     * @return instrument id.
     */
    public int instrumentId()
    {
        return instrumentId;
    }

    /**
     * Encoded private events produced by the command.
     *
     * @return execution output.
     */
    public MessageBatch executionOutput()
    {
        return executionOutput;
    }

    /**
     * Encoded market data produced by the command.
     *
     * @return market data output.
     */
    public MessageBatch marketDataOutput()
    {
        return marketDataOutput;
    }

    /**
     * Clear output after publication.
     */
    public void clearOutput()
    {
        executionOutput.clear();
        marketDataOutput.clear();
    }
}
