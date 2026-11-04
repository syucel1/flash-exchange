package com.flashexchange.protocol;

/**
 * Command: request a full order book snapshot on the market data stream.
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  correlationId
 *      8  4  instrumentId
 * </pre>
 */
public final class BookSnapshotRequestFlyweight extends Flyweight<BookSnapshotRequestFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 3;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 16;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    /** Instrument id meaning every instrument on the engine. */
    public static final int ALL_INSTRUMENTS = 0;

    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int INSTRUMENT_ID_OFFSET = 8;

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
     * Requester supplied correlation id (informational).
     *
     * @return the value.
     */
    public long correlationId()
    {
        return getLong(CORRELATION_ID_OFFSET);
    }

    /**
     * Requester supplied correlation id (informational).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public BookSnapshotRequestFlyweight correlationId(final long value)
    {
        putLong(CORRELATION_ID_OFFSET, value);
        return this;
    }

    /**
     * Instrument to snapshot, or {@link #ALL_INSTRUMENTS}.
     *
     * @return the value.
     */
    public int instrumentId()
    {
        return getInt(INSTRUMENT_ID_OFFSET);
    }

    /**
     * Instrument to snapshot, or {@link #ALL_INSTRUMENTS}.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public BookSnapshotRequestFlyweight instrumentId(final int value)
    {
        putInt(INSTRUMENT_ID_OFFSET, value);
        return this;
    }
}
