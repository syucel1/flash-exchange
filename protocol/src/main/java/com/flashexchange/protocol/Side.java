package com.flashexchange.protocol;

/**
 * Side of an order or the aggressor side of a trade.
 * <p>
 * Encoded on the wire as a single unsigned byte; {@link #get(byte)} returns {@code null} for unknown codes so
 * that decoders never throw on malformed input.
 */
public enum Side
{
    /** Buy (bid) side. */
    BUY((byte)1),

    /** Sell (ask/offer) side. */
    SELL((byte)2);

    private static final Side[] LOOKUP = new Side[256];

    static
    {
        for (final Side value : values())
        {
            LOOKUP[value.code & 0xFF] = value;
        }
    }

    private final byte code;

    Side(final byte code)
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
     * The opposite side.
     *
     * @return {@link #SELL} for {@link #BUY} and vice versa.
     */
    public Side opposite()
    {
        return this == BUY ? SELL : BUY;
    }

    /**
     * Resolve a wire code.
     *
     * @param code read from the wire.
     * @return the matching value or {@code null} if the code is unknown.
     */
    public static Side get(final byte code)
    {
        return LOOKUP[code & 0xFF];
    }
}
