package com.flashexchange.protocol;

/**
 * Command: cancel a resting order (stream 1001, inbound to the engine).
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  clientOrderId
 *      8  8  userId
 *     16  8  orderId
 *     24  8  sendingTime
 *     32  4  instrumentId
 * </pre>
 */
public final class CancelOrderFlyweight extends Flyweight<CancelOrderFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 2;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 40;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    private static final int CLIENT_ORDER_ID_OFFSET = 0;
    private static final int USER_ID_OFFSET = 8;
    private static final int ORDER_ID_OFFSET = 16;
    private static final int SENDING_TIME_OFFSET = 24;
    private static final int INSTRUMENT_ID_OFFSET = 32;

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
     * Client-assigned id of the cancel request.
     *
     * @return the value.
     */
    public long clientOrderId()
    {
        return getLong(CLIENT_ORDER_ID_OFFSET);
    }

    /**
     * Client-assigned id of the cancel request.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public CancelOrderFlyweight clientOrderId(final long value)
    {
        putLong(CLIENT_ORDER_ID_OFFSET, value);
        return this;
    }

    /**
     * User requesting the cancel; must own the order.
     *
     * @return the value.
     */
    public long userId()
    {
        return getLong(USER_ID_OFFSET);
    }

    /**
     * User requesting the cancel; must own the order.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public CancelOrderFlyweight userId(final long value)
    {
        putLong(USER_ID_OFFSET, value);
        return this;
    }

    /**
     * Engine-assigned id of the order to cancel.
     *
     * @return the value.
     */
    public long orderId()
    {
        return getLong(ORDER_ID_OFFSET);
    }

    /**
     * Engine-assigned id of the order to cancel.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public CancelOrderFlyweight orderId(final long value)
    {
        putLong(ORDER_ID_OFFSET, value);
        return this;
    }

    /**
     * Sender timestamp in epoch nanoseconds.
     *
     * @return the value.
     */
    public long sendingTime()
    {
        return getLong(SENDING_TIME_OFFSET);
    }

    /**
     * Sender timestamp in epoch nanoseconds.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public CancelOrderFlyweight sendingTime(final long value)
    {
        putLong(SENDING_TIME_OFFSET, value);
        return this;
    }

    /**
     * Instrument of the order.
     *
     * @return the value.
     */
    public int instrumentId()
    {
        return getInt(INSTRUMENT_ID_OFFSET);
    }

    /**
     * Instrument of the order.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public CancelOrderFlyweight instrumentId(final int value)
    {
        putInt(INSTRUMENT_ID_OFFSET, value);
        return this;
    }
}
