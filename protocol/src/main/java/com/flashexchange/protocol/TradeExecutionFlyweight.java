package com.flashexchange.protocol;

/**
 * Event: one trade between a buy and a sell order (stream 2001). This is the record downstream settlement and portfolio systems consume.
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  engineSequence
 *      8  8  timestamp
 *     16  8  tradeId
 *     24  8  price
 *     32  8  quantity
 *     40  8  buyOrderId
 *     48  8  sellOrderId
 *     56  8  buyUserId
 *     64  8  sellUserId
 *     72  8  buyClientOrderId
 *     80  8  sellClientOrderId
 *     88  4  instrumentId
 *     92  1  aggressorSide
 * </pre>
 */
public final class TradeExecutionFlyweight extends Flyweight<TradeExecutionFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 102;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 96;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    private static final int ENGINE_SEQUENCE_OFFSET = 0;
    private static final int TIMESTAMP_OFFSET = 8;
    private static final int TRADE_ID_OFFSET = 16;
    private static final int PRICE_OFFSET = 24;
    private static final int QUANTITY_OFFSET = 32;
    private static final int BUY_ORDER_ID_OFFSET = 40;
    private static final int SELL_ORDER_ID_OFFSET = 48;
    private static final int BUY_USER_ID_OFFSET = 56;
    private static final int SELL_USER_ID_OFFSET = 64;
    private static final int BUY_CLIENT_ORDER_ID_OFFSET = 72;
    private static final int SELL_CLIENT_ORDER_ID_OFFSET = 80;
    private static final int INSTRUMENT_ID_OFFSET = 88;
    private static final int AGGRESSOR_SIDE_OFFSET = 92;

    @Override
    public int templateId()
    {
        return TEMPLATE_ID;
    }

    @Override
    public int blockLength()
    {
        return BLOCK_LENGTH;
    }

    /**
     * Engine sequence of the command that produced the trade.
     *
     * @return the value.
     */
    public long engineSequence()
    {
        return getLong(ENGINE_SEQUENCE_OFFSET);
    }

    /**
     * Engine sequence of the command that produced the trade.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight engineSequence(final long value)
    {
        putLong(ENGINE_SEQUENCE_OFFSET, value);
        return this;
    }

    /**
     * Engine timestamp (epoch nanoseconds).
     *
     * @return the value.
     */
    public long timestamp()
    {
        return getLong(TIMESTAMP_OFFSET);
    }

    /**
     * Engine timestamp (epoch nanoseconds).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight timestamp(final long value)
    {
        putLong(TIMESTAMP_OFFSET, value);
        return this;
    }

    /**
     * Unique trade id.
     *
     * @return the value.
     */
    public long tradeId()
    {
        return getLong(TRADE_ID_OFFSET);
    }

    /**
     * Unique trade id.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight tradeId(final long value)
    {
        putLong(TRADE_ID_OFFSET, value);
        return this;
    }

    /**
     * Execution price (the resting order's price).
     *
     * @return the value.
     */
    public long price()
    {
        return getLong(PRICE_OFFSET);
    }

    /**
     * Execution price (the resting order's price).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight price(final long value)
    {
        putLong(PRICE_OFFSET, value);
        return this;
    }

    /**
     * Executed quantity.
     *
     * @return the value.
     */
    public long quantity()
    {
        return getLong(QUANTITY_OFFSET);
    }

    /**
     * Executed quantity.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight quantity(final long value)
    {
        putLong(QUANTITY_OFFSET, value);
        return this;
    }

    /**
     * Buy order id.
     *
     * @return the value.
     */
    public long buyOrderId()
    {
        return getLong(BUY_ORDER_ID_OFFSET);
    }

    /**
     * Buy order id.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight buyOrderId(final long value)
    {
        putLong(BUY_ORDER_ID_OFFSET, value);
        return this;
    }

    /**
     * Sell order id.
     *
     * @return the value.
     */
    public long sellOrderId()
    {
        return getLong(SELL_ORDER_ID_OFFSET);
    }

    /**
     * Sell order id.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight sellOrderId(final long value)
    {
        putLong(SELL_ORDER_ID_OFFSET, value);
        return this;
    }

    /**
     * Buyer.
     *
     * @return the value.
     */
    public long buyUserId()
    {
        return getLong(BUY_USER_ID_OFFSET);
    }

    /**
     * Buyer.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight buyUserId(final long value)
    {
        putLong(BUY_USER_ID_OFFSET, value);
        return this;
    }

    /**
     * Seller.
     *
     * @return the value.
     */
    public long sellUserId()
    {
        return getLong(SELL_USER_ID_OFFSET);
    }

    /**
     * Seller.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight sellUserId(final long value)
    {
        putLong(SELL_USER_ID_OFFSET, value);
        return this;
    }

    /**
     * Buyer's client order id.
     *
     * @return the value.
     */
    public long buyClientOrderId()
    {
        return getLong(BUY_CLIENT_ORDER_ID_OFFSET);
    }

    /**
     * Buyer's client order id.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight buyClientOrderId(final long value)
    {
        putLong(BUY_CLIENT_ORDER_ID_OFFSET, value);
        return this;
    }

    /**
     * Seller's client order id.
     *
     * @return the value.
     */
    public long sellClientOrderId()
    {
        return getLong(SELL_CLIENT_ORDER_ID_OFFSET);
    }

    /**
     * Seller's client order id.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight sellClientOrderId(final long value)
    {
        putLong(SELL_CLIENT_ORDER_ID_OFFSET, value);
        return this;
    }

    /**
     * Instrument.
     *
     * @return the value.
     */
    public int instrumentId()
    {
        return getInt(INSTRUMENT_ID_OFFSET);
    }

    /**
     * Instrument.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight instrumentId(final int value)
    {
        putInt(INSTRUMENT_ID_OFFSET, value);
        return this;
    }

    /**
     * Side of the incoming (taker) order.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public Side aggressorSide()
    {
        return Side.get(getByte(AGGRESSOR_SIDE_OFFSET));
    }

    /**
     * Raw wire code of {@link #aggressorSide()}.
     *
     * @return the encoded byte.
     */
    public byte aggressorSideCode()
    {
        return getByte(AGGRESSOR_SIDE_OFFSET);
    }

    /**
     * Side of the incoming (taker) order.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight aggressorSide(final Side value)
    {
        putByte(AGGRESSOR_SIDE_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #aggressorSide()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public TradeExecutionFlyweight aggressorSideCode(final byte code)
    {
        putByte(AGGRESSOR_SIDE_OFFSET, code);
        return this;
    }
}
