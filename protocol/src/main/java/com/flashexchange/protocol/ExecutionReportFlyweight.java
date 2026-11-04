package com.flashexchange.protocol;

/**
 * Event: private execution report for one order (stream 2001, outbound from the engine).
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  engineSequence
 *      8  8  timestamp
 *     16  8  orderId
 *     24  8  clientOrderId
 *     32  8  userId
 *     40  8  price
 *     48  8  quantity
 *     56  8  lastPrice
 *     64  8  lastQuantity
 *     72  8  cumulativeQuantity
 *     80  8  leavesQuantity
 *     88  8  tradeId
 *     96  4  instrumentId
 *    100  1  execType
 *    101  1  orderStatus
 *    102  1  side
 *    103  1  orderType
 *    104  1  timeInForce
 *    105  1  liquidity
 *    106  1  reason
 * </pre>
 */
public final class ExecutionReportFlyweight extends Flyweight<ExecutionReportFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 101;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 112;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    private static final int ENGINE_SEQUENCE_OFFSET = 0;
    private static final int TIMESTAMP_OFFSET = 8;
    private static final int ORDER_ID_OFFSET = 16;
    private static final int CLIENT_ORDER_ID_OFFSET = 24;
    private static final int USER_ID_OFFSET = 32;
    private static final int PRICE_OFFSET = 40;
    private static final int QUANTITY_OFFSET = 48;
    private static final int LAST_PRICE_OFFSET = 56;
    private static final int LAST_QUANTITY_OFFSET = 64;
    private static final int CUMULATIVE_QUANTITY_OFFSET = 72;
    private static final int LEAVES_QUANTITY_OFFSET = 80;
    private static final int TRADE_ID_OFFSET = 88;
    private static final int INSTRUMENT_ID_OFFSET = 96;
    private static final int EXEC_TYPE_OFFSET = 100;
    private static final int ORDER_STATUS_OFFSET = 101;
    private static final int SIDE_OFFSET = 102;
    private static final int ORDER_TYPE_OFFSET = 103;
    private static final int TIME_IN_FORCE_OFFSET = 104;
    private static final int LIQUIDITY_OFFSET = 105;
    private static final int REASON_OFFSET = 106;

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
     * Engine sequence of the command that produced this event.
     *
     * @return the value.
     */
    public long engineSequence()
    {
        return getLong(ENGINE_SEQUENCE_OFFSET);
    }

    /**
     * Engine sequence of the command that produced this event.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight engineSequence(final long value)
    {
        putLong(ENGINE_SEQUENCE_OFFSET, value);
        return this;
    }

    /**
     * Engine timestamp (epoch nanoseconds) of the command that produced this event.
     *
     * @return the value.
     */
    public long timestamp()
    {
        return getLong(TIMESTAMP_OFFSET);
    }

    /**
     * Engine timestamp (epoch nanoseconds) of the command that produced this event.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight timestamp(final long value)
    {
        putLong(TIMESTAMP_OFFSET, value);
        return this;
    }

    /**
     * Engine-assigned order id (0 when an order is rejected before an id is meaningful).
     *
     * @return the value.
     */
    public long orderId()
    {
        return getLong(ORDER_ID_OFFSET);
    }

    /**
     * Engine-assigned order id (0 when an order is rejected before an id is meaningful).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight orderId(final long value)
    {
        putLong(ORDER_ID_OFFSET, value);
        return this;
    }

    /**
     * Client order id (for cancel rejects: the client id of the cancel request).
     *
     * @return the value.
     */
    public long clientOrderId()
    {
        return getLong(CLIENT_ORDER_ID_OFFSET);
    }

    /**
     * Client order id (for cancel rejects: the client id of the cancel request).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight clientOrderId(final long value)
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
    public ExecutionReportFlyweight userId(final long value)
    {
        putLong(USER_ID_OFFSET, value);
        return this;
    }

    /**
     * Limit price of the order (0 for market orders).
     *
     * @return the value.
     */
    public long price()
    {
        return getLong(PRICE_OFFSET);
    }

    /**
     * Limit price of the order (0 for market orders).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight price(final long value)
    {
        putLong(PRICE_OFFSET, value);
        return this;
    }

    /**
     * Original order quantity.
     *
     * @return the value.
     */
    public long quantity()
    {
        return getLong(QUANTITY_OFFSET);
    }

    /**
     * Original order quantity.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight quantity(final long value)
    {
        putLong(QUANTITY_OFFSET, value);
        return this;
    }

    /**
     * Price of this fill (TRADE only).
     *
     * @return the value.
     */
    public long lastPrice()
    {
        return getLong(LAST_PRICE_OFFSET);
    }

    /**
     * Price of this fill (TRADE only).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight lastPrice(final long value)
    {
        putLong(LAST_PRICE_OFFSET, value);
        return this;
    }

    /**
     * Quantity of this fill (TRADE only).
     *
     * @return the value.
     */
    public long lastQuantity()
    {
        return getLong(LAST_QUANTITY_OFFSET);
    }

    /**
     * Quantity of this fill (TRADE only).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight lastQuantity(final long value)
    {
        putLong(LAST_QUANTITY_OFFSET, value);
        return this;
    }

    /**
     * Total filled quantity so far.
     *
     * @return the value.
     */
    public long cumulativeQuantity()
    {
        return getLong(CUMULATIVE_QUANTITY_OFFSET);
    }

    /**
     * Total filled quantity so far.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight cumulativeQuantity(final long value)
    {
        putLong(CUMULATIVE_QUANTITY_OFFSET, value);
        return this;
    }

    /**
     * Quantity still open on the book (0 once filled, cancelled or rejected).
     *
     * @return the value.
     */
    public long leavesQuantity()
    {
        return getLong(LEAVES_QUANTITY_OFFSET);
    }

    /**
     * Quantity still open on the book (0 once filled, cancelled or rejected).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight leavesQuantity(final long value)
    {
        putLong(LEAVES_QUANTITY_OFFSET, value);
        return this;
    }

    /**
     * Trade id of this fill (TRADE only).
     *
     * @return the value.
     */
    public long tradeId()
    {
        return getLong(TRADE_ID_OFFSET);
    }

    /**
     * Trade id of this fill (TRADE only).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight tradeId(final long value)
    {
        putLong(TRADE_ID_OFFSET, value);
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
    public ExecutionReportFlyweight instrumentId(final int value)
    {
        putInt(INSTRUMENT_ID_OFFSET, value);
        return this;
    }

    /**
     * What happened.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public ExecType execType()
    {
        return ExecType.get(getByte(EXEC_TYPE_OFFSET));
    }

    /**
     * Raw wire code of {@link #execType()}.
     *
     * @return the encoded byte.
     */
    public byte execTypeCode()
    {
        return getByte(EXEC_TYPE_OFFSET);
    }

    /**
     * What happened.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight execType(final ExecType value)
    {
        putByte(EXEC_TYPE_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #execType()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight execTypeCode(final byte code)
    {
        putByte(EXEC_TYPE_OFFSET, code);
        return this;
    }

    /**
     * Order status after this event.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public OrderStatus orderStatus()
    {
        return OrderStatus.get(getByte(ORDER_STATUS_OFFSET));
    }

    /**
     * Raw wire code of {@link #orderStatus()}.
     *
     * @return the encoded byte.
     */
    public byte orderStatusCode()
    {
        return getByte(ORDER_STATUS_OFFSET);
    }

    /**
     * Order status after this event.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight orderStatus(final OrderStatus value)
    {
        putByte(ORDER_STATUS_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #orderStatus()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight orderStatusCode(final byte code)
    {
        putByte(ORDER_STATUS_OFFSET, code);
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
    public ExecutionReportFlyweight side(final Side value)
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
    public ExecutionReportFlyweight sideCode(final byte code)
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
    public ExecutionReportFlyweight orderType(final OrderType value)
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
    public ExecutionReportFlyweight orderTypeCode(final byte code)
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
    public ExecutionReportFlyweight timeInForce(final TimeInForce value)
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
    public ExecutionReportFlyweight timeInForceCode(final byte code)
    {
        putByte(TIME_IN_FORCE_OFFSET, code);
        return this;
    }

    /**
     * Maker/taker indicator for fills.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public Liquidity liquidity()
    {
        return Liquidity.get(getByte(LIQUIDITY_OFFSET));
    }

    /**
     * Raw wire code of {@link #liquidity()}.
     *
     * @return the encoded byte.
     */
    public byte liquidityCode()
    {
        return getByte(LIQUIDITY_OFFSET);
    }

    /**
     * Maker/taker indicator for fills.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight liquidity(final Liquidity value)
    {
        putByte(LIQUIDITY_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #liquidity()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight liquidityCode(final byte code)
    {
        putByte(LIQUIDITY_OFFSET, code);
        return this;
    }

    /**
     * Reject/cancel reason.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public Reason reason()
    {
        return Reason.get(getByte(REASON_OFFSET));
    }

    /**
     * Raw wire code of {@link #reason()}.
     *
     * @return the encoded byte.
     */
    public byte reasonCode()
    {
        return getByte(REASON_OFFSET);
    }

    /**
     * Reject/cancel reason.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight reason(final Reason value)
    {
        putByte(REASON_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #reason()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public ExecutionReportFlyweight reasonCode(final byte code)
    {
        putByte(REASON_OFFSET, code);
        return this;
    }
}
