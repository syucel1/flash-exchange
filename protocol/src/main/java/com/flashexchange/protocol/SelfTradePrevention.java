package com.flashexchange.protocol;

/**
 * Self-trade prevention mode applied by the incoming (taker) order when it would match an order of the same user.
 * <p>
 * Encoded on the wire as a single unsigned byte; {@link #get(byte)} returns {@code null} for unknown codes so
 * that decoders never throw on malformed input.
 */
public enum SelfTradePrevention
{
    /** Self trades are allowed. */
    NONE((byte)0),

    /** Cancel the remaining quantity of the incoming order (cancel newest). */
    CANCEL_TAKER((byte)1),

    /** Cancel the resting order and continue matching (cancel oldest). */
    CANCEL_MAKER((byte)2),

    /** Cancel both the resting order and the remainder of the incoming order. */
    CANCEL_BOTH((byte)3);

    private static final SelfTradePrevention[] LOOKUP = new SelfTradePrevention[256];

    static
    {
        for (final SelfTradePrevention value : values())
        {
            LOOKUP[value.code & 0xFF] = value;
        }
    }

    private final byte code;

    SelfTradePrevention(final byte code)
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
    public static SelfTradePrevention get(final byte code)
    {
        return LOOKUP[code & 0xFF];
    }
}
