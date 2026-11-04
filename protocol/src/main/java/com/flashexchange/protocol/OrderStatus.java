package com.flashexchange.protocol;

/**
 * Status of an order after the event reported in an execution report.
 * <p>
 * Encoded on the wire as a single unsigned byte; {@link #get(byte)} returns {@code null} for unknown codes so
 * that decoders never throw on malformed input.
 */
public enum OrderStatus
{
    /** Accepted, nothing filled yet. */
    NEW((byte)1),

    /** Some quantity filled, remainder still open. */
    PARTIALLY_FILLED((byte)2),

    /** Fully filled. */
    FILLED((byte)3),

    /** Cancelled (possibly after partial fills). */
    CANCELED((byte)4),

    /** Rejected. */
    REJECTED((byte)5);

    private static final OrderStatus[] LOOKUP = new OrderStatus[256];

    static
    {
        for (final OrderStatus value : values())
        {
            LOOKUP[value.code & 0xFF] = value;
        }
    }

    private final byte code;

    OrderStatus(final byte code)
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
    public static OrderStatus get(final byte code)
    {
        return LOOKUP[code & 0xFF];
    }
}
