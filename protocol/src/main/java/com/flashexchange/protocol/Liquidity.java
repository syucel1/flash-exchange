package com.flashexchange.protocol;

/**
 * Liquidity indicator of a fill.
 * <p>
 * Encoded on the wire as a single unsigned byte; {@link #get(byte)} returns {@code null} for unknown codes so
 * that decoders never throw on malformed input.
 */
public enum Liquidity
{
    /** Not a fill. */
    NONE((byte)0),

    /** Resting order that provided liquidity. */
    MAKER((byte)1),

    /** Incoming order that removed liquidity. */
    TAKER((byte)2);

    private static final Liquidity[] LOOKUP = new Liquidity[256];

    static
    {
        for (final Liquidity value : values())
        {
            LOOKUP[value.code & 0xFF] = value;
        }
    }

    private final byte code;

    Liquidity(final byte code)
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
    public static Liquidity get(final byte code)
    {
        return LOOKUP[code & 0xFF];
    }
}
