package com.flashexchange.engine.durability;

import io.aeron.ExclusivePublication;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;

/**
 * The open journal: the publication commands are journaled to and the archive's recording position counter, which
 * tells how far the journal is durable.
 */
public final class Journal implements AutoCloseable
{
    private final ExclusivePublication publication;
    private final CountersReader counters;
    private final int counterId;
    private final long recordingId;

    Journal(final ExclusivePublication publication, final CountersReader counters, final int counterId,
        final long recordingId)
    {
        this.publication = publication;
        this.counters = counters;
        this.counterId = counterId;
        this.recordingId = recordingId;
    }

    /**
     * Journal publication.
     *
     * @return publication.
     */
    public ExclusivePublication publication()
    {
        return publication;
    }

    /**
     * Recording id of the journal.
     *
     * @return id.
     */
    public long recordingId()
    {
        return recordingId;
    }

    /**
     * Position up to which the archive has recorded the journal.
     *
     * @return recorded position.
     */
    public long recordedPosition()
    {
        return counters.getCounterValue(counterId);
    }

    /**
     * Position of the journal publication (everything journaled so far).
     *
     * @return position.
     */
    public long position()
    {
        return publication.position();
    }

    @Override
    public void close()
    {
        CloseHelper.close(publication);
    }
}
