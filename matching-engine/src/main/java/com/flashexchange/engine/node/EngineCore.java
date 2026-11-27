package com.flashexchange.engine.node;

import com.flashexchange.engine.config.EngineConfig;
import com.flashexchange.engine.durability.DurabilityController;
import com.flashexchange.engine.durability.EngineArchive;
import com.flashexchange.engine.durability.Journal;
import com.flashexchange.engine.durability.JournalStage;
import com.flashexchange.engine.durability.SnapshotImage;
import com.flashexchange.engine.durability.SnapshotStore;
import com.flashexchange.engine.ha.ReplicationLink;
import com.flashexchange.engine.ha.Sequencer;
import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.engine.pipeline.EnginePipeline;
import com.flashexchange.engine.pipeline.MessagePublisher;
import com.flashexchange.engine.transport.CommandIngress;
import com.flashexchange.protocol.JournalEntryFlyweight;
import com.flashexchange.protocol.MessageHeader;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SystemEpochNanoClock;

import java.util.concurrent.TimeUnit;

/**
 * The deterministic state machine of one engine node: sequencer, Disruptor pipeline (matcher shards, optional
 * journal stage, release gate and publisher) and the command ingress that feeds it.
 * <p>
 * The core is built once the node knows where its state comes from (a local snapshot, a snapshot fetched from the
 * primary, or nothing), so the books can be restored before any consumer thread starts.
 */
public final class EngineCore implements AutoCloseable
{
    private static final System.Logger LOGGER = System.getLogger(EngineCore.class.getName());

    private final EngineConfig config;
    private final Sequencer sequencer;
    private final EnginePipeline pipeline;
    private final CommandIngress ingress;
    private final Journal journal;
    private final JournalStage journalStage;
    private final DurabilityController durability;
    private final SnapshotStore snapshotStore;
    private final JournalEntryFlyweight entry = new JournalEntryFlyweight();
    private long replayedEntries;

    private EngineCore(
        final EngineConfig config,
        final Subscription commandSubscription,
        final MessagePublisher executionPublisher,
        final MessagePublisher marketDataPublisher,
        final EngineArchive archive,
        final Journal journal,
        final ReplicationLink link,
        final SnapshotImage snapshot)
    {
        this.config = config;
        this.journal = journal;
        this.sequencer = new Sequencer(config.durability().dedupWindow());
        if (journal != null)
        {
            this.journalStage = new JournalStage(journal, config.publishRetryTimeoutMs() * 10);
            this.snapshotStore = new SnapshotStore(archive, config.durability().snapshotsRetained());
            this.durability = new DurabilityController(journal, journalStage, snapshotStore);
            this.durability.replicationGate(link);
        }
        else
        {
            this.journalStage = null;
            this.snapshotStore = null;
            this.durability = null;
        }

        this.pipeline = new EnginePipeline(
            config.instruments(),
            config.matcherShards(),
            config.ringBufferSize(),
            config.waitStrategy().create(),
            config.orderPoolSize(),
            config.snapshotDepth(),
            executionPublisher,
            marketDataPublisher,
            config.shutdownTimeoutMs(),
            journalStage,
            durability,
            config.ha().nodeId());
        if (journalStage != null)
        {
            journalStage.releasedSequence(pipeline.publisher()::lastReleasedEngineSequence);
        }
        if (snapshot != null)
        {
            restore(snapshot);
        }
        this.ingress = new CommandIngress(
            commandSubscription,
            pipeline.start(),
            new SystemEpochNanoClock(),
            config.fragmentLimit(),
            config.instruments().stream().mapToInt(Instrument::id).toArray(),
            TimeUnit.MILLISECONDS.toNanos(config.snapshotIntervalMs()),
            sequencer);
    }

    /**
     * Build and start a core.
     *
     * @param config              engine configuration.
     * @param commandSubscription command stream (1001).
     * @param executionPublisher  private event output.
     * @param marketDataPublisher market data output.
     * @param archive             archive when the journal is enabled, else {@code null}.
     * @param journal             open journal, or {@code null} for an in-memory engine.
     * @param link                replication gate (HA), or {@code null}.
     * @param snapshot            state to restore, or {@code null} to start empty.
     * @return running core.
     */
    public static EngineCore start(
        final EngineConfig config,
        final Subscription commandSubscription,
        final MessagePublisher executionPublisher,
        final MessagePublisher marketDataPublisher,
        final EngineArchive archive,
        final Journal journal,
        final ReplicationLink link,
        final SnapshotImage snapshot)
    {
        return new EngineCore(config, commandSubscription, executionPublisher, marketDataPublisher, archive, journal,
            link, snapshot);
    }

    private void restore(final SnapshotImage snapshot)
    {
        final int shards = pipeline.shardCount();
        if (snapshot.sectionCount() != shards + 1)
        {
            throw new IllegalStateException("snapshot has " + (snapshot.sectionCount() - 1) +
                " shard sections but the engine runs " + shards + " matcher shards");
        }
        sequencer.read(snapshot.section(0), 0);
        for (int shard = 0; shard < shards; shard++)
        {
            pipeline.engine(shard).readState(snapshot.section(shard + 1), 0);
        }
        LOGGER.log(System.Logger.Level.INFO, "restored snapshot " + snapshot.recordingId() + " at sequence " +
            snapshot.snapshotSequence());
    }

    /**
     * Replay a range of the local journal through the pipeline (cold recovery). Entries are marked as replayed so
     * they are not journaled again; their output is published again (consumers drop duplicates by sequence).
     *
     * @param archive       local archive.
     * @param recordingId   journal recording.
     * @param fromPosition  first position (the snapshot's journal position).
     * @param toPosition    end position (the journal's stop position before this run).
     * @return entries replayed.
     */
    public long replayJournal(
        final EngineArchive archive, final long recordingId, final long fromPosition, final long toPosition)
    {
        if (toPosition <= fromPosition)
        {
            return 0;
        }
        final long[] position = {fromPosition};
        final long before = replayedEntries;
        final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) ->
        {
            position[0] = header.position();
            onJournalEntry(buffer, offset, length, true);
        });
        try (Subscription replay = archive.client().replay(recordingId, fromPosition, toPosition - fromPosition,
            EngineArchive.IPC, EngineArchive.REPLAY_STREAM_ID))
        {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (position[0] < toPosition)
            {
                if (replay.poll(assembler, 256) == 0)
                {
                    if (System.nanoTime() - deadline > 0)
                    {
                        throw new IllegalStateException("journal replay stalled at " + position[0] + " of " +
                            toPosition);
                    }
                    Thread.onSpinWait();
                }
            }
        }
        final long count = replayedEntries - before;
        LOGGER.log(System.Logger.Level.INFO, "replayed " + count + " journal entries [" + fromPosition + ", " +
            toPosition + ")");
        return count;
    }

    /**
     * Feed one journal entry (from local replay or from the primary) into the ring.
     *
     * @param buffer containing the message.
     * @param offset of the message header.
     * @param length of the message.
     * @param replay true for local recovery (do not journal again).
     * @return true if the message was a journal entry.
     */
    public boolean onJournalEntry(final DirectBuffer buffer, final int offset, final int length,
        final boolean replay)
    {
        if (length < JournalEntryFlyweight.ENCODED_LENGTH ||
            MessageHeader.templateId(buffer, offset) != JournalEntryFlyweight.TEMPLATE_ID)
        {
            return false;
        }
        entry.wrapForDecode(buffer, offset);
        ingress.publishJournalEntry(entry, replay);
        replayedEntries++;
        return true;
    }

    /**
     * Schedule a state snapshot (producer thread only).
     */
    public void snapshot()
    {
        if (durability != null)
        {
            ingress.publishStateSnapshot(pipeline.shardCount());
        }
    }

    /**
     * Whether state snapshots are written (journal enabled).
     *
     * @return true when durable.
     */
    public boolean isDurable()
    {
        return durability != null;
    }

    /**
     * Configuration.
     *
     * @return config.
     */
    public EngineConfig config()
    {
        return config;
    }

    /**
     * Sequencer (producer-thread state).
     *
     * @return sequencer.
     */
    public Sequencer sequencer()
    {
        return sequencer;
    }

    /**
     * Pipeline.
     *
     * @return pipeline.
     */
    public EnginePipeline pipeline()
    {
        return pipeline;
    }

    /**
     * Command ingress (ring producer).
     *
     * @return ingress.
     */
    public CommandIngress ingress()
    {
        return ingress;
    }

    /**
     * Journal, or {@code null}.
     *
     * @return journal.
     */
    public Journal journal()
    {
        return journal;
    }

    /**
     * Journal stage, or {@code null}.
     *
     * @return stage.
     */
    public JournalStage journalStage()
    {
        return journalStage;
    }

    /**
     * Snapshot store, or {@code null}.
     *
     * @return store.
     */
    public SnapshotStore snapshotStore()
    {
        return snapshotStore;
    }

    /**
     * Entries fed from a journal (replay or replication).
     *
     * @return count.
     */
    public long journalEntriesApplied()
    {
        return replayedEntries;
    }

    /**
     * Stop the pipeline abruptly without draining (crash simulation).
     */
    public void halt()
    {
        if (durability != null)
        {
            durability.close();
            snapshotStore.close();
        }
        pipeline.halt();
        CloseHelper.quietClose(journal);
    }

    /**
     * Drain and stop the pipeline, then close the journal.
     */
    @Override
    public void close()
    {
        pipeline.close();
        if (durability != null)
        {
            durability.close();
        }
        CloseHelper.quietClose(journal);
    }
}
