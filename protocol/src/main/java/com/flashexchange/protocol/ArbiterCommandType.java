package com.flashexchange.protocol;

/**
 * Command sent by the arbiter to an engine node.
 * <p>
 * Encoded on the wire as a single unsigned byte; {@link #get(byte)} returns {@code null} for unknown codes so
 * that decoders never throw on malformed input.
 */
public enum ArbiterCommandType
{
    /** Become primary for the given epoch (grants the first lease). */
    PROMOTE((byte)1),

    /** Become standby of the given primary. */
    FOLLOW((byte)2),

    /** Renew the primary lease. */
    LEASE((byte)3),

    /** Stop processing immediately (a newer epoch exists). */
    FENCE((byte)4);

    private static final ArbiterCommandType[] LOOKUP = new ArbiterCommandType[256];

    static
    {
        for (final ArbiterCommandType value : values())
        {
            LOOKUP[value.code & 0xFF] = value;
        }
    }

    private final byte code;

    ArbiterCommandType(final byte code)
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
    public static ArbiterCommandType get(final byte code)
    {
        return LOOKUP[code & 0xFF];
    }
}
