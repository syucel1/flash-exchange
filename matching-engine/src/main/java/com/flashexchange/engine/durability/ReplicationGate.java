package com.flashexchange.engine.durability;

/**
 * Replication side of the release gate (implemented by the HA layer).
 */
public interface ReplicationGate
{
    /**
     * Primary: wait until the standby acknowledged a sequence, or until the gate decides the output may be
     * released without it (no standby attached, or the ack timed out and the node still holds a valid lease).
     *
     * @param engineSequence sequence about to be released.
     * @return true to publish the output, false to withhold it because the node has been fenced.
     */
    boolean awaitAck(long engineSequence);

    /**
     * Standby: acknowledge that everything up to a sequence is durable in the local journal.
     *
     * @param engineSequence highest durable sequence.
     */
    void sendAck(long engineSequence);
}
