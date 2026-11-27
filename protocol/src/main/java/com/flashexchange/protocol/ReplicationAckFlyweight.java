package com.flashexchange.protocol;

/**
 * Standby to primary (stream 102): every journal entry up to {@link #ackedSequence()} has been
 * applied and durably journaled by the standby.
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  ackedSequence
 *      8  8  epoch
 *     16  4  nodeId
 * </pre>
 */
public final class ReplicationAckFlyweight extends Flyweight<ReplicationAckFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 51;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 24;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    private static final int ACKED_SEQUENCE_OFFSET = 0;
    private static final int EPOCH_OFFSET = 8;
    private static final int NODE_ID_OFFSET = 16;

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
     * Highest contiguous engine sequence durably applied by the standby.
     *
     * @return the value.
     */
    public long ackedSequence()
    {
        return getLong(ACKED_SEQUENCE_OFFSET);
    }

    /**
     * Highest contiguous engine sequence durably applied by the standby.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ReplicationAckFlyweight ackedSequence(final long value)
    {
        putLong(ACKED_SEQUENCE_OFFSET, value);
        return this;
    }

    /**
     * Epoch the standby follows.
     *
     * @return the value.
     */
    public long epoch()
    {
        return getLong(EPOCH_OFFSET);
    }

    /**
     * Epoch the standby follows.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ReplicationAckFlyweight epoch(final long value)
    {
        putLong(EPOCH_OFFSET, value);
        return this;
    }

    /**
     * Standby node id.
     *
     * @return the value.
     */
    public int nodeId()
    {
        return getInt(NODE_ID_OFFSET);
    }

    /**
     * Standby node id.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ReplicationAckFlyweight nodeId(final int value)
    {
        putInt(NODE_ID_OFFSET, value);
        return this;
    }
}
