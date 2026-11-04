package com.flashexchange.protocol;

/**
 * Kind of execution report.
 * <p>
 * Encoded on the wire as a single unsigned byte; {@link #get(byte)} returns {@code null} for unknown codes so
 * that decoders never throw on malformed input.
 */
public enum ExecType
{
    /** Order accepted by the engine. */
    NEW((byte)1),

    /** Order (partially) filled. */
    TRADE((byte)2),

    /** Order (or its remainder) cancelled. */
    CANCELED((byte)3),

    /** Order rejected by validation, never entered the book. */
    REJECTED((byte)4),

    /** Cancel request rejected. */
    CANCEL_REJECTED((byte)5);

    private static final ExecType[] LOOKUP = new ExecType[256];

    static
    {
        for (final ExecType value : values())
        {
            LOOKUP[value.code & 0xFF] = value;
        }
    }

    private final byte code;

    ExecType(final byte code)
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
    public static ExecType get(final byte code)
    {
        return LOOKUP[code & 0xFF];
    }
}
