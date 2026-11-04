package com.flashexchange.protocol;

/**
 * Order type accepted by the matching engine.
 * <p>
 * Encoded on the wire as a single unsigned byte; {@link #get(byte)} returns {@code null} for unknown codes so
 * that decoders never throw on malformed input.
 */
public enum OrderType
{
    /** Limit order: executes at the limit price or better, may rest on the book. */
    LIMIT((byte)1),

    /** Market order: executes immediately against resting liquidity, never rests. */
    MARKET((byte)2);

    private static final OrderType[] LOOKUP = new OrderType[256];

    static
    {
        for (final OrderType value : values())
        {
            LOOKUP[value.code & 0xFF] = value;
        }
    }

    private final byte code;

    OrderType(final byte code)
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
    public static OrderType get(final byte code)
    {
        return LOOKUP[code & 0xFF];
    }
}
