package com.flashexchange.engine.durability;

import com.flashexchange.engine.pipeline.CommandType;
import com.flashexchange.engine.pipeline.OrderCommand;
import com.flashexchange.protocol.JournalEntryFlyweight;
import com.lmax.disruptor.EventHandler;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Disruptor stage running in parallel with the matchers: appends every state-changing command to the journal
 * publication (recorded by the archive) and stamps the slot with the resulting journal position, which the
 * release gate waits on. Replayed commands are already journaled and are skipped. A state snapshot slot is stamped
 * with the journal position it corresponds to.
 * <p>
 * The journal is never allowed to drop an entry: back pressure is retried. If the journal cannot be written for
 * {@code failTimeoutMs} the stage marks itself failed and the release gate withholds all further output.
 */
public final class JournalStage implements EventHandler<OrderCommand>
{
    private final ExclusivePublication publication;
    private final long recordingId;
    private final UnsafeBuffer buffer =
        new UnsafeBuffer(ByteBuffer.allocateDirect(JournalEntryFlyweight.ENCODED_LENGTH + 64));
    private final JournalEntryFlyweight entry = new JournalEntryFlyweight();
    private final IdleStrategy idle = new BackoffIdleStrategy();
    private final long failTimeoutNs;
    private volatile LongSupplier releasedSequence = () -> -1;
    private final AtomicLong entriesJournaled = new AtomicLong();
    private volatile boolean failed;

    /**
     * Create the stage.
     *
     * @param journal       open journal.
     * @param failTimeoutMs how long to retry an unwritable journal before failing.
     */
    public JournalStage(final Journal journal, final long failTimeoutMs)
    {
        this(journal.publication(), journal.recordingId(), failTimeoutMs);
    }

    JournalStage(final ExclusivePublication publication, final long recordingId, final long failTimeoutMs)
    {
        this.publication = publication;
        this.recordingId = recordingId;
        this.failTimeoutNs = TimeUnit.MILLISECONDS.toNanos(failTimeoutMs);
        entry.wrapForEncode(buffer, 0);
    }

    /**
     * Source of the released sequence stamped on entries (the publisher's).
     *
     * @param supplier released sequence supplier.
     */
    public void releasedSequence(final LongSupplier supplier)
    {
        this.releasedSequence = supplier;
    }

    @Override
    public void onEvent(final OrderCommand command, final long sequence, final boolean endOfBatch)
    {
        final CommandType type = command.type();
        if (type == CommandType.STATE_SNAPSHOT)
        {
            command.journalPosition(publication.position());
            return;
        }
        if (type == null || !type.isJournaled() || command.isReplay() || failed)
        {
            return;
        }

        command.toJournal(entry, Math.max(releasedSequence.getAsLong(), command.releasedSequence()));
        final long deadline = System.nanoTime() + failTimeoutNs;
        long result;
        idle.reset();
        while ((result = publication.offer(buffer, 0, JournalEntryFlyweight.ENCODED_LENGTH)) < 0)
        {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED ||
                System.nanoTime() - deadline > 0)
            {
                failed = true;
                throw new IllegalStateException("journal write failed: " + Publication.errorString(result));
            }
            idle.idle();
        }
        command.journalPosition(result);
        entriesJournaled.lazySet(entriesJournaled.get() + 1);
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
     * Entries written.
     *
     * @return count.
     */
    public long entriesJournaled()
    {
        return entriesJournaled.get();
    }

    /**
     * Whether the journal could not be written; output is withheld from then on.
     *
     * @return failed flag.
     */
    public boolean isFailed()
    {
        return failed;
    }
}
