package com.flashexchange.protocol;

/**
 * First record of a state snapshot recording (stream 110).
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  snapshotSequence
 *      8  8  epoch
 *     16  8  journalRecordingId
 *     24  8  journalPosition
 *     32  8  timestamp
 *     40  4  sectionCount
 * </pre>
 */
public final class SnapshotBeginFlyweight extends Flyweight<SnapshotBeginFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 60;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 48;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    private static final int SNAPSHOT_SEQUENCE_OFFSET = 0;
    private static final int EPOCH_OFFSET = 8;
    private static final int JOURNAL_RECORDING_ID_OFFSET = 16;
    private static final int JOURNAL_POSITION_OFFSET = 24;
    private static final int TIMESTAMP_OFFSET = 32;
    private static final int SECTION_COUNT_OFFSET = 40;

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
     * Engine sequence the snapshot reflects (all commands up to and including it).
     *
     * @return the value.
     */
    public long snapshotSequence()
    {
        return getLong(SNAPSHOT_SEQUENCE_OFFSET);
    }

    /**
     * Engine sequence the snapshot reflects (all commands up to and including it).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public SnapshotBeginFlyweight snapshotSequence(final long value)
    {
        putLong(SNAPSHOT_SEQUENCE_OFFSET, value);
        return this;
    }

    /**
     * Epoch at snapshot time.
     *
     * @return the value.
     */
    public long epoch()
    {
        return getLong(EPOCH_OFFSET);
    }

    /**
     * Epoch at snapshot time.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public SnapshotBeginFlyweight epoch(final long value)
    {
        putLong(EPOCH_OFFSET, value);
        return this;
    }

    /**
     * Recording id of the journal the snapshot belongs to.
     *
     * @return the value.
     */
    public long journalRecordingId()
    {
        return getLong(JOURNAL_RECORDING_ID_OFFSET);
    }

    /**
     * Recording id of the journal the snapshot belongs to.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public SnapshotBeginFlyweight journalRecordingId(final long value)
    {
        putLong(JOURNAL_RECORDING_ID_OFFSET, value);
        return this;
    }

    /**
     * Journal position after the last entry included in the snapshot.
     *
     * @return the value.
     */
    public long journalPosition()
    {
        return getLong(JOURNAL_POSITION_OFFSET);
    }

    /**
     * Journal position after the last entry included in the snapshot.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public SnapshotBeginFlyweight journalPosition(final long value)
    {
        putLong(JOURNAL_POSITION_OFFSET, value);
        return this;
    }

    /**
     * Engine timestamp of the snapshot command.
     *
     * @return the value.
     */
    public long timestamp()
    {
        return getLong(TIMESTAMP_OFFSET);
    }

    /**
     * Engine timestamp of the snapshot command.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public SnapshotBeginFlyweight timestamp(final long value)
    {
        putLong(TIMESTAMP_OFFSET, value);
        return this;
    }

    /**
     * Number of state sections that follow (sequencer + one per matcher shard).
     *
     * @return the value.
     */
    public int sectionCount()
    {
        return getInt(SECTION_COUNT_OFFSET);
    }

    /**
     * Number of state sections that follow (sequencer + one per matcher shard).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public SnapshotBeginFlyweight sectionCount(final int value)
    {
        putInt(SECTION_COUNT_OFFSET, value);
        return this;
    }
}
