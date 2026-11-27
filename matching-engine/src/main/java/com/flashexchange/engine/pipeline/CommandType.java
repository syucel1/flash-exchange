package com.flashexchange.engine.pipeline;

/**
 * Kind of command carried by an {@link OrderCommand} ring slot. Only {@link #NEW_ORDER} and {@link #CANCEL_ORDER}
 * change book state; they are the only commands that consume an engine sequence and are journaled.
 */
public enum CommandType
{
    /** Submit a new order. */
    NEW_ORDER((byte)1),

    /** Cancel a resting order. */
    CANCEL_ORDER((byte)2),

    /** Publish a market data snapshot of one book (not journaled, reuses the last engine sequence). */
    BOOK_SNAPSHOT((byte)3),

    /** Capture a consistent state snapshot of every shard (not journaled). */
    STATE_SNAPSHOT((byte)4),

    /** The node changed role or epoch; the publisher announces it and adjusts its release mode (not journaled). */
    ROLE_CHANGE((byte)5);

    private final byte code;

    CommandType(final byte code)
    {
        this.code = code;
    }

    /**
     * Wire code used in journal entries.
     *
     * @return code.
     */
    public byte code()
    {
        return code;
    }

    /**
     * Whether commands of this type are journaled and consume an engine sequence.
     *
     * @return true for state-changing commands.
     */
    public boolean isJournaled()
    {
        return this == NEW_ORDER || this == CANCEL_ORDER;
    }

    /**
     * Resolve a journaled command type from its wire code.
     *
     * @param code wire code.
     * @return the type.
     * @throws IllegalArgumentException if the code is not a journaled command type.
     */
    public static CommandType journaled(final byte code)
    {
        if (code == NEW_ORDER.code)
        {
            return NEW_ORDER;
        }
        if (code == CANCEL_ORDER.code)
        {
            return CANCEL_ORDER;
        }
        throw new IllegalArgumentException("not a journaled command type: " + code);
    }
}
