package com.flashexchange.protocol;

/**
 * Event on streams 2001 and 3001: a node started acting as primary for an epoch. Clients use it to
 * discard re-published messages they have already seen (engine sequence at or below
 * {@link #lastSequenceBefore()}).
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  epoch
 *      8  8  lastSequenceBefore
 *     16  8  timestamp
 *     24  4  nodeId
 *     28  1  role
 * </pre>
 */
public final class EngineRoleFlyweight extends Flyweight<EngineRoleFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 130;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 32;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    private static final int EPOCH_OFFSET = 0;
    private static final int LAST_SEQUENCE_BEFORE_OFFSET = 8;
    private static final int TIMESTAMP_OFFSET = 16;
    private static final int NODE_ID_OFFSET = 24;
    private static final int ROLE_OFFSET = 28;

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
     * New epoch.
     *
     * @return the value.
     */
    public long epoch()
    {
        return getLong(EPOCH_OFFSET);
    }

    /**
     * New epoch.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public EngineRoleFlyweight epoch(final long value)
    {
        putLong(EPOCH_OFFSET, value);
        return this;
    }

    /**
     * Highest engine sequence released before the takeover (best knowledge).
     *
     * @return the value.
     */
    public long lastSequenceBefore()
    {
        return getLong(LAST_SEQUENCE_BEFORE_OFFSET);
    }

    /**
     * Highest engine sequence released before the takeover (best knowledge).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public EngineRoleFlyweight lastSequenceBefore(final long value)
    {
        putLong(LAST_SEQUENCE_BEFORE_OFFSET, value);
        return this;
    }

    /**
     * Engine clock (epoch nanoseconds).
     *
     * @return the value.
     */
    public long timestamp()
    {
        return getLong(TIMESTAMP_OFFSET);
    }

    /**
     * Engine clock (epoch nanoseconds).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public EngineRoleFlyweight timestamp(final long value)
    {
        putLong(TIMESTAMP_OFFSET, value);
        return this;
    }

    /**
     * Primary node id.
     *
     * @return the value.
     */
    public int nodeId()
    {
        return getInt(NODE_ID_OFFSET);
    }

    /**
     * Primary node id.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public EngineRoleFlyweight nodeId(final int value)
    {
        putInt(NODE_ID_OFFSET, value);
        return this;
    }

    /**
     * Role (PRIMARY).
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public NodeRole role()
    {
        return NodeRole.get(getByte(ROLE_OFFSET));
    }

    /**
     * Raw wire code of {@link #role()}.
     *
     * @return the encoded byte.
     */
    public byte roleCode()
    {
        return getByte(ROLE_OFFSET);
    }

    /**
     * Role (PRIMARY).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public EngineRoleFlyweight role(final NodeRole value)
    {
        putByte(ROLE_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #role()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public EngineRoleFlyweight roleCode(final byte code)
    {
        putByte(ROLE_OFFSET, code);
        return this;
    }
}
