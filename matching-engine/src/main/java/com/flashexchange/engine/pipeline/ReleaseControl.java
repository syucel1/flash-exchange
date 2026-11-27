package com.flashexchange.engine.pipeline;

/**
 * Durability hooks of the publisher stage (the release gate). Implemented by the durability layer; absent for an
 * in-memory engine.
 */
public interface ReleaseControl
{
    /**
     * Block until the output of a command may leave the engine: its journal entry is durable in the local archive
     * and, for a primary in SYNC mode, acknowledged by the standby.
     *
     * @param command   slot about to be released.
     * @param retaining true when this node is a standby (output is retained, not published).
     * @return true when the output may be published (or retained), false when it must be dropped (fenced).
     */
    boolean awaitRelease(OrderCommand command, boolean retaining);

    /**
     * Persist a state snapshot whose sections were filled by the producer and the matchers.
     *
     * @param command state snapshot slot.
     */
    void onStateSnapshot(OrderCommand command);

    /**
     * End of a batch of released (or retained) commands. A standby acknowledges the sequence to the primary.
     *
     * @param engineSequence highest engine sequence handled.
     * @param retaining      true when this node is a standby.
     */
    void onBatchReleased(long engineSequence, boolean retaining);
}
