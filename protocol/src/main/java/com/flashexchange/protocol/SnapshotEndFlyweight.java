package com.flashexchange.protocol;

/**
 * Last record of a state snapshot recording; a snapshot without it is incomplete and ignored.
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  snapshotSequence
 *      8  8  checksum
 *     16  8  totalBytes
 * </pre>
 */
public final class SnapshotEndFlyweight extends Flyweight<SnapshotEndFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 62;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 24;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    private static final int SNAPSHOT_SEQUENCE_OFFSET = 0;
    private static final int CHECKSUM_OFFSET = 8;
    private static final int TOTAL_BYTES_OFFSET = 16;

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
     * Engine sequence of the snapshot.
     *
     * @return the value.
     */
    public long snapshotSequence()
    {
        return getLong(SNAPSHOT_SEQUENCE_OFFSET);
    }

    /**
     * Engine sequence of the snapshot.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public SnapshotEndFlyweight snapshotSequence(final long value)
    {
        putLong(SNAPSHOT_SEQUENCE_OFFSET, value);
        return this;
    }

    /**
     * CRC32 over all section payloads in order.
     *
     * @return the value.
     */
    public long checksum()
    {
        return getLong(CHECKSUM_OFFSET);
    }

    /**
     * CRC32 over all section payloads in order.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public SnapshotEndFlyweight checksum(final long value)
    {
        putLong(CHECKSUM_OFFSET, value);
        return this;
    }

    /**
     * Total payload bytes.
     *
     * @return the value.
     */
    public long totalBytes()
    {
        return getLong(TOTAL_BYTES_OFFSET);
    }

    /**
     * Total payload bytes.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public SnapshotEndFlyweight totalBytes(final long value)
    {
        putLong(TOTAL_BYTES_OFFSET, value);
        return this;
    }
}
