package com.flashexchange.protocol;

/**
 * Reason code carried by rejected, cancelled and cancel-rejected execution reports.
 * <p>
 * Encoded on the wire as a single unsigned byte; {@link #get(byte)} returns {@code null} for unknown codes so
 * that decoders never throw on malformed input.
 */
public enum Reason
{
    /** No reason (normal execution). */
    NONE((byte)0),

    /** Instrument is not configured on this engine. */
    UNKNOWN_INSTRUMENT((byte)1),

    /** Side missing or unknown. */
    INVALID_SIDE((byte)2),

    /** Order type missing or unknown. */
    INVALID_ORDER_TYPE((byte)3),

    /** Time in force missing, unknown or not valid for the order type. */
    INVALID_TIME_IN_FORCE((byte)4),

    /** Quantity not positive or not a multiple of the lot size. */
    INVALID_QUANTITY((byte)5),

    /** Quantity below the instrument minimum. */
    BELOW_MIN_QUANTITY((byte)6),

    /** Quantity above the instrument maximum. */
    ABOVE_MAX_QUANTITY((byte)7),

    /** Limit price not positive, not a tick multiple, or a price given on a market order. */
    INVALID_PRICE((byte)8),

    /** Post-only order would have matched on entry. */
    POST_ONLY_WOULD_TAKE((byte)9),

    /** Self-trade prevention mode unknown. */
    INVALID_SELF_TRADE_PREVENTION((byte)10),

    /** Fill-or-kill order could not be fully filled. */
    FOK_NOT_FILLABLE((byte)11),

    /** Market order remainder cancelled because the opposite side ran out. */
    NO_LIQUIDITY((byte)12),

    /** Market order remainder cancelled at the price protection collar. */
    PRICE_COLLAR((byte)13),

    /** Immediate-or-cancel remainder cancelled. */
    IOC_REMAINDER((byte)14),

    /** Cancelled by user request. */
    USER_CANCEL((byte)15),

    /** Cancelled by self-trade prevention. */
    SELF_TRADE_PREVENTION((byte)16),

    /** Order not found (never existed, already filled or already cancelled). */
    UNKNOWN_ORDER((byte)17),

    /** Cancel request from a user that does not own the order. */
    NOT_ORDER_OWNER((byte)18),

    /** Post-only flag on a market order. */
    POST_ONLY_NOT_ALLOWED((byte)19);

    private static final Reason[] LOOKUP = new Reason[256];

    static
    {
        for (final Reason value : values())
        {
            LOOKUP[value.code & 0xFF] = value;
        }
    }

    private final byte code;

    Reason(final byte code)
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
    public static Reason get(final byte code)
    {
        return LOOKUP[code & 0xFF];
    }
}
