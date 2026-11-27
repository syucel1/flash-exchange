package com.flashexchange.protocol;

/**
 * Journal/replication record: one sequenced command exactly as the engine processed it (stream 100
 * recorded by the Aeron Archive, stream 101 to the standby). Replaying these records in order through the engine
 * reproduces its state and output deterministically.
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
 *     56  8  epoch
 *     64  4  instrumentId
 *     68  1  commandType
 *     69  1  side
 *     70  1  orderType
 *     71  1  timeInForce
 *     72  1  selfTradePrevention
 *     73  1  flags
 *     74  6  (padding)
 *     80  8  releasedSequence
 * </pre>
 */
public final class JournalEntryFlyweight extends Flyweight<JournalEntryFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 50;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 88;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    /** Flag bit: post-only order. */
    public static final byte FLAG_POST_ONLY = 0x01;

    /** Flag bit: the sequencer detected a duplicate client order id; the order is rejected. */
    public static final byte FLAG_DUPLICATE = 0x02;

    private static final int ENGINE_SEQUENCE_OFFSET = 0;
    private static final int TIMESTAMP_OFFSET = 8;
    private static final int ORDER_ID_OFFSET = 16;
    private static final int CLIENT_ORDER_ID_OFFSET = 24;
    private static final int USER_ID_OFFSET = 32;
    private static final int PRICE_OFFSET = 40;
    private static final int QUANTITY_OFFSET = 48;
    private static final int EPOCH_OFFSET = 56;
    private static final int INSTRUMENT_ID_OFFSET = 64;
    private static final int COMMAND_TYPE_OFFSET = 68;
    private static final int SIDE_OFFSET = 69;
    private static final int ORDER_TYPE_OFFSET = 70;
    private static final int TIME_IN_FORCE_OFFSET = 71;
    private static final int SELF_TRADE_PREVENTION_OFFSET = 72;
    private static final int FLAGS_OFFSET = 73;
    private static final int RELEASED_SEQUENCE_OFFSET = 80;

    /**
     * Whether {@link #FLAG_POST_ONLY} is set.
     *
     * @return true for post-only orders.
     */
    public boolean isPostOnly()
    {
        return (flags() & FLAG_POST_ONLY) != 0;
    }

    /**
     * Whether {@link #FLAG_DUPLICATE} is set.
     *
     * @return true when the sequencer flagged a duplicate client order id.
     */
    public boolean isDuplicate()
    {
        return (flags() & FLAG_DUPLICATE) != 0;
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
     * Engine sequence assigned by the sequencer.
     *
     * @return the value.
     */
    public long engineSequence()
    {
        return getLong(ENGINE_SEQUENCE_OFFSET);
    }

    /**
     * Engine sequence assigned by the sequencer.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight engineSequence(final long value)
    {
        putLong(ENGINE_SEQUENCE_OFFSET, value);
        return this;
    }

    /**
     * Engine timestamp assigned by the sequencer (epoch nanoseconds).
     *
     * @return the value.
     */
    public long timestamp()
    {
        return getLong(TIMESTAMP_OFFSET);
    }

    /**
     * Engine timestamp assigned by the sequencer (epoch nanoseconds).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight timestamp(final long value)
    {
        putLong(TIMESTAMP_OFFSET, value);
        return this;
    }

    /**
     * Order id assigned to a new order, or the target order of a cancel.
     *
     * @return the value.
     */
    public long orderId()
    {
        return getLong(ORDER_ID_OFFSET);
    }

    /**
     * Order id assigned to a new order, or the target order of a cancel.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight orderId(final long value)
    {
        putLong(ORDER_ID_OFFSET, value);
        return this;
    }

    /**
     * Client order id (new order) or client id of the cancel request.
     *
     * @return the value.
     */
    public long clientOrderId()
    {
        return getLong(CLIENT_ORDER_ID_OFFSET);
    }

    /**
     * Client order id (new order) or client id of the cancel request.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight clientOrderId(final long value)
    {
        putLong(CLIENT_ORDER_ID_OFFSET, value);
        return this;
    }

    /**
     * User.
     *
     * @return the value.
     */
    public long userId()
    {
        return getLong(USER_ID_OFFSET);
    }

    /**
     * User.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight userId(final long value)
    {
        putLong(USER_ID_OFFSET, value);
        return this;
    }

    /**
     * Limit price.
     *
     * @return the value.
     */
    public long price()
    {
        return getLong(PRICE_OFFSET);
    }

    /**
     * Limit price.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight price(final long value)
    {
        putLong(PRICE_OFFSET, value);
        return this;
    }

    /**
     * Quantity.
     *
     * @return the value.
     */
    public long quantity()
    {
        return getLong(QUANTITY_OFFSET);
    }

    /**
     * Quantity.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight quantity(final long value)
    {
        putLong(QUANTITY_OFFSET, value);
        return this;
    }

    /**
     * Epoch of the primary that sequenced the command.
     *
     * @return the value.
     */
    public long epoch()
    {
        return getLong(EPOCH_OFFSET);
    }

    /**
     * Epoch of the primary that sequenced the command.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight epoch(final long value)
    {
        putLong(EPOCH_OFFSET, value);
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
    public JournalEntryFlyweight instrumentId(final int value)
    {
        putInt(INSTRUMENT_ID_OFFSET, value);
        return this;
    }

    /**
     * Command type code (see {@code CommandType} in the engine).
     *
     * @return the value.
     */
    public byte commandType()
    {
        return getByte(COMMAND_TYPE_OFFSET);
    }

    /**
     * Command type code (see {@code CommandType} in the engine).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight commandType(final byte value)
    {
        putByte(COMMAND_TYPE_OFFSET, value);
        return this;
    }

    /**
     * Side wire code as received.
     *
     * @return the value.
     */
    public byte side()
    {
        return getByte(SIDE_OFFSET);
    }

    /**
     * Side wire code as received.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight side(final byte value)
    {
        putByte(SIDE_OFFSET, value);
        return this;
    }

    /**
     * Order type wire code as received.
     *
     * @return the value.
     */
    public byte orderType()
    {
        return getByte(ORDER_TYPE_OFFSET);
    }

    /**
     * Order type wire code as received.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight orderType(final byte value)
    {
        putByte(ORDER_TYPE_OFFSET, value);
        return this;
    }

    /**
     * Time in force wire code as received.
     *
     * @return the value.
     */
    public byte timeInForce()
    {
        return getByte(TIME_IN_FORCE_OFFSET);
    }

    /**
     * Time in force wire code as received.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight timeInForce(final byte value)
    {
        putByte(TIME_IN_FORCE_OFFSET, value);
        return this;
    }

    /**
     * Self-trade prevention wire code as received.
     *
     * @return the value.
     */
    public byte selfTradePrevention()
    {
        return getByte(SELF_TRADE_PREVENTION_OFFSET);
    }

    /**
     * Self-trade prevention wire code as received.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight selfTradePrevention(final byte value)
    {
        putByte(SELF_TRADE_PREVENTION_OFFSET, value);
        return this;
    }

    /**
     * Bit flags, see {@link #FLAG_POST_ONLY} and {@link #FLAG_DUPLICATE}.
     *
     * @return the value.
     */
    public byte flags()
    {
        return getByte(FLAGS_OFFSET);
    }

    /**
     * Bit flags, see {@link #FLAG_POST_ONLY} and {@link #FLAG_DUPLICATE}.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight flags(final byte value)
    {
        putByte(FLAGS_OFFSET, value);
        return this;
    }

    /**
     * Highest engine sequence whose output the writer had released to clients when it journaled this entry. A
     * standby uses it to bound how much retained output it must re-publish on promotion.
     *
     * @return released sequence, -1 if none.
     */
    public long releasedSequence()
    {
        return getLong(RELEASED_SEQUENCE_OFFSET);
    }

    /**
     * Highest engine sequence whose output the writer had released to clients when it journaled this entry.
     *
     * @param value released sequence.
     * @return this for fluent use.
     */
    public JournalEntryFlyweight releasedSequence(final long value)
    {
        putLong(RELEASED_SEQUENCE_OFFSET, value);
        return this;
    }
}
