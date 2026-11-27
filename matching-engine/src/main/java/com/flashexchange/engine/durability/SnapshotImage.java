package com.flashexchange.engine.durability;

import org.agrona.concurrent.UnsafeBuffer;

/**
 * A complete, verified state snapshot: section 0 is the sequencer, sections 1..n the matcher shards.
 *
 * @param recordingId        archive recording holding the snapshot.
 * @param snapshotSequence   last engine sequence included.
 * @param epoch              epoch the snapshot was taken in.
 * @param journalRecordingId journal recording the snapshot refers to.
 * @param journalPosition    journal position to replay from.
 * @param timestamp          engine time of the snapshot.
 * @param sections           section payloads.
 */
public record SnapshotImage(
    long recordingId,
    long snapshotSequence,
    long epoch,
    long journalRecordingId,
    long journalPosition,
    long timestamp,
    byte[][] sections)
{
    /**
     * Section as a buffer.
     *
     * @param index section.
     * @return buffer over the payload.
     */
    public UnsafeBuffer section(final int index)
    {
        return new UnsafeBuffer(sections[index]);
    }

    /**
     * Number of sections.
     *
     * @return count.
     */
    public int sectionCount()
    {
        return sections.length;
    }
}
