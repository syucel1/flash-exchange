package com.flashexchange.protocol;

/**
 * Command: submit a new limit or market order (stream 1001, inbound to the engine).
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  clientOrderId
 *      8  8  userId
 *     16  8  price
 *     24  8  quantity
 *     32  8  sendingTime
 *     40  4  instrumentId
 *     44  1  side
 *     45  1  orderType
 *     46  1  timeInForce
 *     47  1  selfTradePrevention
 *     48  1  flags
 * </pre>
 */
public final class NewOrderFlyweight extends Flyweight<NewOrderFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 1;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 56;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    /** Flag bit: order must not take liquidity on entry (limit orders only). */
    public static final byte FLAG_POST_ONLY = 0x01;

    private static final int CLIENT_ORDER_ID_OFFSET = 0;
    private static final int USER_ID_OFFSET = 8;
    private static final int PRICE_OFFSET = 16;
    private static final int QUANTITY_OFFSET = 24;
    private static final int SENDING_TIME_OFFSET = 32;
    private static final int INSTRUMENT_ID_OFFSET = 40;
    private static final int SIDE_OFFSET = 44;
    private static final int ORDER_TYPE_OFFSET = 45;
    private static final int TIME_IN_FORCE_OFFSET = 46;
    private static final int SELF_TRADE_PREVENTION_OFFSET = 47;
    private static final int FLAGS_OFFSET = 48;

    /**
     * Whether {@link #FLAG_POST_ONLY} is set.
     *
     * @return true for post-only orders.
     */
    public boolean isPostOnly()
    {
        return (flags() & FLAG_POST_ONLY) != 0;
    }

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
     * Client-assigned order id, echoed in every execution report for the order.
     *
     * @return the value.
     */
    public long clientOrderId()
    {
        return getLong(CLIENT_ORDER_ID_OFFSET);
    }

    /**
     * Client-assigned order id, echoed in every execution report for the order.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight clientOrderId(final long value)
    {
        putLong(CLIENT_ORDER_ID_OFFSET, value);
        return this;
    }

    /**
     * Owning user.
     *
     * @return the value.
     */
    public long userId()
    {
        return getLong(USER_ID_OFFSET);
    }

    /**
     * Owning user.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight userId(final long value)
    {
        putLong(USER_ID_OFFSET, value);
        return this;
    }

    /**
     * Limit price in instrument price units (ticks scaled by price scale); 0 for market orders.
     *
     * @return the value.
     */
    public long price()
    {
        return getLong(PRICE_OFFSET);
    }

    /**
     * Limit price in instrument price units (ticks scaled by price scale); 0 for market orders.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight price(final long value)
    {
        putLong(PRICE_OFFSET, value);
        return this;
    }

    /**
     * Order quantity in instrument quantity units.
     *
     * @return the value.
     */
    public long quantity()
    {
        return getLong(QUANTITY_OFFSET);
    }

    /**
     * Order quantity in instrument quantity units.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight quantity(final long value)
    {
        putLong(QUANTITY_OFFSET, value);
        return this;
    }

    /**
     * Sender timestamp in epoch nanoseconds (latency tracing only, not used for priority).
     *
     * @return the value.
     */
    public long sendingTime()
    {
        return getLong(SENDING_TIME_OFFSET);
    }

    /**
     * Sender timestamp in epoch nanoseconds (latency tracing only, not used for priority).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight sendingTime(final long value)
    {
        putLong(SENDING_TIME_OFFSET, value);
        return this;
    }

    /**
     * Instrument to trade.
     *
     * @return the value.
     */
    public int instrumentId()
    {
        return getInt(INSTRUMENT_ID_OFFSET);
    }

    /**
     * Instrument to trade.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight instrumentId(final int value)
    {
        putInt(INSTRUMENT_ID_OFFSET, value);
        return this;
    }

    /**
     * Order side.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public Side side()
    {
        return Side.get(getByte(SIDE_OFFSET));
    }

    /**
     * Raw wire code of {@link #side()}.
     *
     * @return the encoded byte.
     */
    public byte sideCode()
    {
        return getByte(SIDE_OFFSET);
    }

    /**
     * Order side.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight side(final Side value)
    {
        putByte(SIDE_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #side()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight sideCode(final byte code)
    {
        putByte(SIDE_OFFSET, code);
        return this;
    }

    /**
     * Order type.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public OrderType orderType()
    {
        return OrderType.get(getByte(ORDER_TYPE_OFFSET));
    }

    /**
     * Raw wire code of {@link #orderType()}.
     *
     * @return the encoded byte.
     */
    public byte orderTypeCode()
    {
        return getByte(ORDER_TYPE_OFFSET);
    }

    /**
     * Order type.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight orderType(final OrderType value)
    {
        putByte(ORDER_TYPE_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #orderType()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight orderTypeCode(final byte code)
    {
        putByte(ORDER_TYPE_OFFSET, code);
        return this;
    }

    /**
     * Time in force.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public TimeInForce timeInForce()
    {
        return TimeInForce.get(getByte(TIME_IN_FORCE_OFFSET));
    }

    /**
     * Raw wire code of {@link #timeInForce()}.
     *
     * @return the encoded byte.
     */
    public byte timeInForceCode()
    {
        return getByte(TIME_IN_FORCE_OFFSET);
    }

    /**
     * Time in force.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight timeInForce(final TimeInForce value)
    {
        putByte(TIME_IN_FORCE_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #timeInForce()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight timeInForceCode(final byte code)
    {
        putByte(TIME_IN_FORCE_OFFSET, code);
        return this;
    }

    /**
     * Self-trade prevention mode.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public SelfTradePrevention selfTradePrevention()
    {
        return SelfTradePrevention.get(getByte(SELF_TRADE_PREVENTION_OFFSET));
    }

    /**
     * Raw wire code of {@link #selfTradePrevention()}.
     *
     * @return the encoded byte.
     */
    public byte selfTradePreventionCode()
    {
        return getByte(SELF_TRADE_PREVENTION_OFFSET);
    }

    /**
     * Self-trade prevention mode.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight selfTradePrevention(final SelfTradePrevention value)
    {
        putByte(SELF_TRADE_PREVENTION_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #selfTradePrevention()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight selfTradePreventionCode(final byte code)
    {
        putByte(SELF_TRADE_PREVENTION_OFFSET, code);
        return this;
    }

    /**
     * Bit flags, see {@link #FLAG_POST_ONLY}.
     *
     * @return the value.
     */
    public byte flags()
    {
        return getByte(FLAGS_OFFSET);
    }

    /**
     * Bit flags, see {@link #FLAG_POST_ONLY}.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public NewOrderFlyweight flags(final byte value)
    {
        putByte(FLAGS_OFFSET, value);
        return this;
    }
}
