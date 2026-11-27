package com.flashexchange.protocol;

/**
 * Role of a matching engine node.
 * <p>
 * Encoded on the wire as a single unsigned byte; {@link #get(byte)} returns {@code null} for unknown codes so
 * that decoders never throw on malformed input.
 */
public enum NodeRole
{
    /** Started, waiting for the arbiter to assign a role. */
    PENDING((byte)0),

    /** Single node without replication (accepts commands). */
    STANDALONE((byte)1),

    /** Sequences and processes commands, replicates to the standby. */
    PRIMARY((byte)2),

    /** Applies the primary's journal; takes over on failover. */
    STANDBY((byte)3),

    /** Lost its lease or saw a higher epoch; no longer processes commands. */
    FENCED((byte)4);

    private static final NodeRole[] LOOKUP = new NodeRole[256];

    static
    {
        for (final NodeRole value : values())
        {
            LOOKUP[value.code & 0xFF] = value;
        }
    }

    private final byte code;

    NodeRole(final byte code)
    {
        this.code = code;
    }

    /**
     * Wire code of this value.
     *
     * @return the byte written to the wire.
     */
    public byte code()
    {
        return code;
    }

    /**
     * Resolve a wire code.
     *
     * @param code read from the wire.
     * @return the matching value or {@code null} if the code is unknown.
     */
    public static NodeRole get(final byte code)
    {
        return LOOKUP[code & 0xFF];
    }
}
