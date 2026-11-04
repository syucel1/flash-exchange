package com.flashexchange.protocol;

/**
 * How long an order remains active.
 * <p>
 * Encoded on the wire as a single unsigned byte; {@link #get(byte)} returns {@code null} for unknown codes so
 * that decoders never throw on malformed input.
 */
public enum TimeInForce
{
    /** Good till cancel: unfilled quantity of a limit order rests on the book until filled or cancelled. */
    GTC((byte)1),

    /** Immediate or cancel: fill what is possible immediately, cancel the remainder. */
    IOC((byte)2),

    /** Fill or kill: fill the full quantity immediately or cancel the whole order. */
    FOK((byte)3);

    private static final TimeInForce[] LOOKUP = new TimeInForce[256];

    static
    {
        for (final TimeInForce value : values())
        {
            LOOKUP[value.code & 0xFF] = value;
        }
    }

    private final byte code;

    TimeInForce(final byte code)
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
    public static TimeInForce get(final byte code)
    {
        return LOOKUP[code & 0xFF];
    }
}
