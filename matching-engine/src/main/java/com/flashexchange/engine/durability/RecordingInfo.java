package com.flashexchange.engine.durability;

/**
 * Descriptor of an Aeron Archive recording.
 *
 * @param recordingId      recording id.
 * @param startTimestamp   start time (epoch ms).
 * @param startPosition    first recorded position.
 * @param stopPosition     last recorded position, or {@code -1} while recording.
 * @param initialTermId    initial term id of the recorded stream.
 * @param termBufferLength term length of the recorded stream.
 * @param mtuLength        MTU of the recorded stream.
 * @param sessionId        session id of the recorded stream.
 * @param streamId         stream id.
 */
public record RecordingInfo(
    long recordingId,
    long startTimestamp,
    long startPosition,
    long stopPosition,
    int initialTermId,
    int termBufferLength,
    int mtuLength,
    int sessionId,
    int streamId)
{
    /**
     * Whether the recording has been stopped (its stop position is known).
     *
     * @return true when stopped.
     */
    public boolean isStopped()
    {
        return stopPosition >= 0;
    }
}
