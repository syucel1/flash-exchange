package com.flashexchange.engine.pipeline;

/**
 * Kind of command carried by an {@link OrderCommand} ring slot.
 */
public enum CommandType
{
    /** Submit a new order. */
    NEW_ORDER,

    /** Cancel a resting order. */
    CANCEL_ORDER,

    /** Publish a snapshot of one book. */
    BOOK_SNAPSHOT
}
