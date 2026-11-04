package com.flashexchange.engine.matching;

import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;

/**
 * Mutable, pooled order. Instances are owned by a single matching thread and recycled through
 * {@link ObjectPool}; references must not be retained by event sinks beyond the callback.
 */
public final class Order
{
    long orderId;
    long clientOrderId;
    long userId;
    int instrumentId;
    Side side;
    OrderType orderType;
    TimeInForce timeInForce;
    SelfTradePrevention selfTradePrevention;
    long price;
    long quantity;
    long filledQuantity;
    long timestamp;

    Order prev;
    Order next;
    PriceLevel level;

    void reset()
    {
        orderId = 0;
        clientOrderId = 0;
        userId = 0;
        instrumentId = 0;
        side = null;
        orderType = null;
        timeInForce = null;
        selfTradePrevention = null;
        price = 0;
        quantity = 0;
        filledQuantity = 0;
        timestamp = 0;
        prev = null;
        next = null;
        level = null;
    }

    /**
     * Engine-assigned order id.
     *
     * @return order id.
     */
    public long orderId()
    {
        return orderId;
    }

    /**
     * Client-assigned order id.
     *
     * @return client order id.
     */
    public long clientOrderId()
    {
        return clientOrderId;
    }

    /**
     * Owning user.
     *
     * @return user id.
     */
    public long userId()
    {
        return userId;
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
     * Side.
     *
     * @return side.
     */
    public Side side()
    {
        return side;
    }

    /**
     * Order type.
     *
     * @return order type.
     */
    public OrderType orderType()
    {
        return orderType;
    }

    /**
     * Time in force.
     *
     * @return time in force.
     */
    public TimeInForce timeInForce()
    {
        return timeInForce;
    }

    /**
     * Self-trade prevention mode.
     *
     * @return mode.
     */
    public SelfTradePrevention selfTradePrevention()
    {
        return selfTradePrevention;
    }

    /**
     * Limit price (0 for market orders).
     *
     * @return price.
     */
    public long price()
    {
        return price;
    }

    /**
     * Original quantity.
     *
     * @return quantity.
     */
    public long quantity()
    {
        return quantity;
    }

    /**
     * Filled quantity.
     *
     * @return filled quantity.
     */
    public long filledQuantity()
    {
        return filledQuantity;
    }

    /**
     * Quantity not yet filled.
     *
     * @return remaining quantity.
     */
    public long remainingQuantity()
    {
        return quantity - filledQuantity;
    }

    /**
     * Engine timestamp at which the order was accepted.
     *
     * @return epoch nanoseconds.
     */
    public long timestamp()
    {
        return timestamp;
    }

    /**
     * Whether the order is currently resting on a book.
     *
     * @return true if resting.
     */
    public boolean isResting()
    {
        return level != null;
    }

    @Override
    public String toString()
    {
        return "Order{id=" + orderId + ", clOrdId=" + clientOrderId + ", user=" + userId + ", " + side + " " +
            remainingQuantity() + "/" + quantity + " @ " + price + "}";
    }
}
