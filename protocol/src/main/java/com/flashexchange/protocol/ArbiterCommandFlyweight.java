package com.flashexchange.protocol;

/**
 * Arbiter to engine node: role assignment and leases.
 * <p>
 * Root block layout (little-endian, offsets relative to the end of the {@link MessageHeader}):
 * <pre>
 *      0  8  epoch
 *      8  8  leaseDurationMs
 *     16  4  targetNodeId
 *     20  4  primaryNodeId
 *     24  1  command
 * </pre>
 */
public final class ArbiterCommandFlyweight extends Flyweight<ArbiterCommandFlyweight>
{
    /** Template id. */
    public static final int TEMPLATE_ID = 53;

    /** Root block length. */
    public static final int BLOCK_LENGTH = 32;

    /** Total encoded length (header + root block). */
    public static final int ENCODED_LENGTH = MessageHeader.LENGTH + BLOCK_LENGTH;

    private static final int EPOCH_OFFSET = 0;
    private static final int LEASE_DURATION_MS_OFFSET = 8;
    private static final int TARGET_NODE_ID_OFFSET = 16;
    private static final int PRIMARY_NODE_ID_OFFSET = 20;
    private static final int COMMAND_OFFSET = 24;

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
     * Epoch the command belongs to.
     *
     * @return the value.
     */
    public long epoch()
    {
        return getLong(EPOCH_OFFSET);
    }

    /**
     * Epoch the command belongs to.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ArbiterCommandFlyweight epoch(final long value)
    {
        putLong(EPOCH_OFFSET, value);
        return this;
    }

    /**
     * For PROMOTE/LEASE: how long the primary may act without a new lease.
     *
     * @return the value.
     */
    public long leaseDurationMs()
    {
        return getLong(LEASE_DURATION_MS_OFFSET);
    }

    /**
     * For PROMOTE/LEASE: how long the primary may act without a new lease.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ArbiterCommandFlyweight leaseDurationMs(final long value)
    {
        putLong(LEASE_DURATION_MS_OFFSET, value);
        return this;
    }

    /**
     * Node the command is for.
     *
     * @return the value.
     */
    public int targetNodeId()
    {
        return getInt(TARGET_NODE_ID_OFFSET);
    }

    /**
     * Node the command is for.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ArbiterCommandFlyweight targetNodeId(final int value)
    {
        putInt(TARGET_NODE_ID_OFFSET, value);
        return this;
    }

    /**
     * Current primary (for FOLLOW).
     *
     * @return the value.
     */
    public int primaryNodeId()
    {
        return getInt(PRIMARY_NODE_ID_OFFSET);
    }

    /**
     * Current primary (for FOLLOW).
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ArbiterCommandFlyweight primaryNodeId(final int value)
    {
        putInt(PRIMARY_NODE_ID_OFFSET, value);
        return this;
    }

    /**
     * Command.
     *
     * @return the value, or {@code null} if the wire code is unknown.
     */
    public ArbiterCommandType command()
    {
        return ArbiterCommandType.get(getByte(COMMAND_OFFSET));
    }

    /**
     * Raw wire code of {@link #command()}.
     *
     * @return the encoded byte.
     */
    public byte commandCode()
    {
        return getByte(COMMAND_OFFSET);
    }

    /**
     * Command.
     *
     * @param value to encode.
     * @return this for fluent use.
     */
    public ArbiterCommandFlyweight command(final ArbiterCommandType value)
    {
        putByte(COMMAND_OFFSET, value.code());
        return this;
    }

    /**
     * Encode a raw wire code for {@link #command()} (used to model malformed input in tests and tools).
     *
     * @param code to encode.
     * @return this for fluent use.
     */
    public ArbiterCommandFlyweight commandCode(final byte code)
    {
        putByte(COMMAND_OFFSET, code);
        return this;
    }
}
