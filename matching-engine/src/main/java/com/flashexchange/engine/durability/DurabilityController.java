package com.flashexchange.engine.durability;

import com.flashexchange.engine.pipeline.OrderCommand;
import com.flashexchange.engine.pipeline.ReleaseControl;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * Release gate of a durable engine: output leaves the engine only once its command is recorded in the local
 * journal (and, for an HA primary, acknowledged by the standby per {@link ReplicationGate}). Also writes the state
 * snapshots the producer schedules. Runs on the publisher thread.
 */
public final class DurabilityController implements ReleaseControl
{
    private static final System.Logger LOGGER = System.getLogger(DurabilityController.class.getName());

    private final Journal journal;
    private final JournalStage journalStage;
    private final SnapshotStore snapshotStore;
    private final IdleStrategy idle = new BackoffIdleStrategy();
    private volatile ReplicationGate replicationGate;
    private volatile boolean closed;
    private volatile long lastDurableAck = -1;

    /**
     * Create the controller.
     *
     * @param journal       open journal.
     * @param journalStage  stage writing the journal (failure withholds output).
     * @param snapshotStore snapshot writer.
     */
    public DurabilityController(final Journal journal, final JournalStage journalStage,
        final SnapshotStore snapshotStore)
    {
        this.journal = journal;
        this.journalStage = journalStage;
        this.snapshotStore = snapshotStore;
    }

    /**
     * Attach (or detach with {@code null}) the replication gate.
     *
     * @param gate replication gate.
     */
    public void replicationGate(final ReplicationGate gate)
    {
        this.replicationGate = gate;
    }

    @Override
    public boolean awaitRelease(final OrderCommand command, final boolean retaining)
    {
        awaitDurable(command.journalPosition());
        final ReplicationGate gate = replicationGate;
        if (!retaining && gate != null && !command.isReplay() && command.type() != null &&
            command.type().isJournaled())
        {
            return gate.awaitAck(command.engineSequence());
        }
        return true;
    }

    private void awaitDurable(final long position)
    {
        idle.reset();
        while (journalStage.isFailed() || (position > 0 && journal.recordedPosition() < position))
        {
            if (closed)
            {
                throw new IllegalStateException("engine closing: output withheld");
            }
            idle.idle();
        }
    }

    @Override
    public void onStateSnapshot(final OrderCommand command)
    {
        awaitDurable(command.journalPosition());
        try
        {
            final long recordingId = snapshotStore.write(command, journal.recordingId());
            LOGGER.log(System.Logger.Level.INFO, "state snapshot at sequence " + command.engineSequence() +
                " journal position " + command.journalPosition() + " recording " + recordingId);
        }
        catch (final RuntimeException ex)
        {
            LOGGER.log(System.Logger.Level.ERROR, "state snapshot failed", ex);
        }
    }

    @Override
    public void onBatchReleased(final long engineSequence, final boolean retaining)
    {
        final ReplicationGate gate = replicationGate;
        if (retaining && gate != null && engineSequence > lastDurableAck)
        {
            gate.sendAck(engineSequence);
            lastDurableAck = engineSequence;
        }
    }

    /**
     * Stop waiting (engine shutdown).
     */
    public void close()
    {
        closed = true;
    }

    /**
     * Journal.
     *
     * @return journal.
     */
    public Journal journal()
    {
        return journal;
    }

    /**
     * Journal stage.
     *
     * @return stage.
     */
    public JournalStage journalStage()
    {
        return journalStage;
    }

    /**
     * Snapshot store.
     *
     * @return store.
     */
    public SnapshotStore snapshotStore()
    {
        return snapshotStore;
    }
}
