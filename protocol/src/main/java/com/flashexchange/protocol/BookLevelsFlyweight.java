package com.flashexchange.protocol;

import static com.flashexchange.protocol.MessageHeader.BYTE_ORDER;

/**
 * Common base for market data messages that carry price levels in repeating groups
 * ({@link BookUpdateFlyweight} and {@link BookSnapshotFlyweight}).
 * <p>
 * Root block layout:
 * <pre>
 *      0  8  engineSequence
 *      8  8  timestamp
 *     16  8  bookSequence
 *     24  4  instrumentId
 *     28  4  (padding)
 * </pre>
 * Each repeating group starts with a 4 byte group header ({@code blockLength} u16, {@code numInGroup} u16)
 * followed by {@code numInGroup} entries of {@link #ENTRY_LENGTH} bytes:
 * <pre>
 *      0  8  price
 *      8  8  quantity    (total quantity at the level, 0 = level removed)
 *     16  4  orderCount
 *     20  1  side
 *     21  3  (padding)
 * </pre>
 *
 * @param <T> concrete type.
 */
public abstract class BookLevelsFlyweight<T extends BookLevelsFlyweight<T>> extends Flyweight<T>
{
    /** Root block length. */
    public static final int BLOCK_LENGTH = 32;

    /** Length of a repeating group header. */
    public static final int GROUP_HEADER_LENGTH = 4;

    /** Length of one level entry. */
    public static final int ENTRY_LENGTH = 24;

    /** Maximum number of entries in one group. */
    public static final int MAX_ENTRIES = 0xFFFF;

    private static final int ENGINE_SEQUENCE_OFFSET = 0;
    private static final int TIMESTAMP_OFFSET = 8;
    private static final int BOOK_SEQUENCE_OFFSET = 16;
    private static final int INSTRUMENT_ID_OFFSET = 24;

    private static final int ENTRY_PRICE_OFFSET = 0;
    private static final int ENTRY_QUANTITY_OFFSET = 8;
    private static final int ENTRY_ORDER_COUNT_OFFSET = 16;
    private static final int ENTRY_SIDE_OFFSET = 20;

    @Override
    public int blockLength()
    {
        return BLOCK_LENGTH;
    }

    /**
     * Engine sequence of the command that produced this message.
     *
     * @return the value.
     */
    public long engineSequence()
    {
        return getLong(ENGINE_SEQUENCE_OFFSET);
    }

    /**
     * Engine sequence of the command that produced this message.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    @SuppressWarnings("unchecked")
    public T engineSequence(final long value)
    {
        putLong(ENGINE_SEQUENCE_OFFSET, value);
        return (T)this;
    }

    /**
     * Engine timestamp in epoch nanoseconds.
     *
     * @return the value.
     */
    public long timestamp()
    {
        return getLong(TIMESTAMP_OFFSET);
    }

    /**
     * Engine timestamp in epoch nanoseconds.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    @SuppressWarnings("unchecked")
    public T timestamp(final long value)
    {
        putLong(TIMESTAMP_OFFSET, value);
        return (T)this;
    }

    /**
     * Per-instrument book sequence. Incremented by one for every {@link BookUpdateFlyweight}; a snapshot carries
     * the sequence of the last update it includes.
     *
     * @return the value.
     */
    public long bookSequence()
    {
        return getLong(BOOK_SEQUENCE_OFFSET);
    }

    /**
     * Per-instrument book sequence.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    @SuppressWarnings("unchecked")
    public T bookSequence(final long value)
    {
        putLong(BOOK_SEQUENCE_OFFSET, value);
        return (T)this;
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
    @SuppressWarnings("unchecked")
    public T instrumentId(final int value)
    {
        putInt(INSTRUMENT_ID_OFFSET, value);
        return (T)this;
    }

    /**
     * Absolute offset of the first group header.
     *
     * @return offset in the buffer.
     */
    protected final int firstGroupOffset()
    {
        return offset + actingBlockLength;
    }

    /**
     * Encoded length of a group starting at the given offset.
     *
     * @param groupOffset absolute offset of the group header.
     * @return group length including header.
     */
    protected final int groupLength(final int groupOffset)
    {
        return GROUP_HEADER_LENGTH + groupCount(groupOffset) * groupEntryLength(groupOffset);
    }

    /**
     * Number of entries of the group at the given offset.
     *
     * @param groupOffset absolute offset of the group header.
     * @return number of entries.
     */
    protected final int groupCount(final int groupOffset)
    {
        return buffer.getShort(groupOffset + 2, BYTE_ORDER) & 0xFFFF;
    }

    private int groupEntryLength(final int groupOffset)
    {
        return buffer.getShort(groupOffset, BYTE_ORDER) & 0xFFFF;
    }

    /**
     * Write a group header.
     *
     * @param groupOffset absolute offset of the group header.
     * @param count       number of entries that will follow.
     */
    protected final void putGroupHeader(final int groupOffset, final int count)
    {
        if (count < 0 || count > MAX_ENTRIES)
        {
            throw new IllegalArgumentException("group count out of range: " + count);
        }
        mutableBuffer.putShort(groupOffset, (short)ENTRY_LENGTH, BYTE_ORDER);
        mutableBuffer.putShort(groupOffset + 2, (short)count, BYTE_ORDER);
    }

    /**
     * Write a level entry.
     *
     * @param groupOffset absolute offset of the group header.
     * @param index       of the entry.
     * @param side        of the level.
     * @param price       of the level.
     * @param quantity    total quantity at the level (0 = removed).
     * @param orderCount  number of orders at the level.
     */
    protected final void putEntry(
        final int groupOffset,
        final int index,
        final Side side,
        final long price,
        final long quantity,
        final int orderCount)
    {
        final int entryOffset = groupOffset + GROUP_HEADER_LENGTH + index * ENTRY_LENGTH;
        mutableBuffer.putLong(entryOffset + ENTRY_PRICE_OFFSET, price, BYTE_ORDER);
        mutableBuffer.putLong(entryOffset + ENTRY_QUANTITY_OFFSET, quantity, BYTE_ORDER);
        mutableBuffer.putInt(entryOffset + ENTRY_ORDER_COUNT_OFFSET, orderCount, BYTE_ORDER);
        mutableBuffer.putByte(entryOffset + ENTRY_SIDE_OFFSET, side.code());
        mutableBuffer.putByte(entryOffset + ENTRY_SIDE_OFFSET + 1, (byte)0);
        mutableBuffer.putShort(entryOffset + ENTRY_SIDE_OFFSET + 2, (short)0, BYTE_ORDER);
    }

    private int entryOffset(final int groupOffset, final int index)
    {
        if (index < 0 || index >= groupCount(groupOffset))
        {
            throw new IndexOutOfBoundsException("index=" + index + " count=" + groupCount(groupOffset));
        }
        return groupOffset + GROUP_HEADER_LENGTH + index * groupEntryLength(groupOffset);
    }

    /**
     * Price of an entry.
     *
     * @param groupOffset absolute offset of the group header.
     * @param index       of the entry.
     * @return the price.
     */
    protected final long entryPrice(final int groupOffset, final int index)
    {
        return buffer.getLong(entryOffset(groupOffset, index) + ENTRY_PRICE_OFFSET, BYTE_ORDER);
    }

    /**
     * Quantity of an entry.
     *
     * @param groupOffset absolute offset of the group header.
     * @param index       of the entry.
     * @return the total quantity at the level.
     */
    protected final long entryQuantity(final int groupOffset, final int index)
    {
        return buffer.getLong(entryOffset(groupOffset, index) + ENTRY_QUANTITY_OFFSET, BYTE_ORDER);
    }

    /**
     * Order count of an entry.
     *
     * @param groupOffset absolute offset of the group header.
     * @param index       of the entry.
     * @return the number of orders at the level.
     */
    protected final int entryOrderCount(final int groupOffset, final int index)
    {
        return buffer.getInt(entryOffset(groupOffset, index) + ENTRY_ORDER_COUNT_OFFSET, BYTE_ORDER);
    }

    /**
     * Side of an entry.
     *
     * @param groupOffset absolute offset of the group header.
     * @param index       of the entry.
     * @return the side or {@code null} if unknown.
     */
    protected final Side entrySide(final int groupOffset, final int index)
    {
        return Side.get(buffer.getByte(entryOffset(groupOffset, index) + ENTRY_SIDE_OFFSET));
    }
}
