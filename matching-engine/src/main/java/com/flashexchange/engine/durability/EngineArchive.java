package com.flashexchange.engine.durability;

import com.flashexchange.engine.config.DurabilityConfig;
import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The engine's embedded Aeron Archive: holds the command journal (stream 100) and the state snapshots (stream 110)
 * of this node. It runs on the engine's media driver; when an archive control port is configured the peer can
 * connect to it remotely to bootstrap and follow the journal (replication).
 */
public final class EngineArchive implements AutoCloseable
{
    /** Journal stream id. */
    public static final int JOURNAL_STREAM_ID = 100;

    /** Snapshot stream id. */
    public static final int SNAPSHOT_STREAM_ID = 110;

    /** Local replay stream id. */
    public static final int REPLAY_STREAM_ID = 120;

    /** Recorded channel of the journal and snapshots. */
    public static final String IPC = "aeron:ipc";

    private static final AtomicInteger RESPONSE_STREAMS = new AtomicInteger(20_100);
    private static final long AWAIT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    private final Archive archive;
    private final Aeron aeron;
    private final AeronArchive client;
    private final int archiveId;
    private final File archiveDirectory;

    private EngineArchive(final Archive archive, final Aeron aeron, final AeronArchive client, final int archiveId,
        final File archiveDirectory)
    {
        this.archive = archive;
        this.aeron = aeron;
        this.client = client;
        this.archiveId = archiveId;
        this.archiveDirectory = archiveDirectory;
    }

    /**
     * Launch the archive on an existing media driver and connect a local client.
     *
     * @param config        durability settings.
     * @param archiveId     unique archive id (the node id).
     * @param aeronDirectory media driver directory.
     * @param aeron         engine Aeron client (used by the archive client).
     * @return the running archive.
     */
    public static EngineArchive launch(
        final DurabilityConfig config, final int archiveId, final String aeronDirectory, final Aeron aeron)
    {
        final File directory = new File(config.dataDirectory(), "archive");
        if (!directory.exists() && !directory.mkdirs())
        {
            throw new IllegalStateException("cannot create archive directory " + directory);
        }
        // An engine owns its data directory exclusively; a mark file left by a killed process is stale.
        final File markFile = new File(directory, "archive-mark.dat");
        if (markFile.exists() && !markFile.delete())
        {
            throw new IllegalStateException("cannot remove stale archive mark file " + markFile);
        }

        final Archive.Context context = new Archive.Context()
            .aeronDirectoryName(aeronDirectory)
            .archiveDir(directory)
            .archiveId(archiveId)
            .deleteArchiveOnStart(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .fileSyncLevel(config.fileSyncLevel())
            .catalogFileSyncLevel(config.fileSyncLevel())
            .recordingEventsEnabled(false)
            .archiveClientContext(new AeronArchive.Context().controlResponseChannel(IPC))
            .replicationChannel("aeron:udp?endpoint=" + config.archiveHost() + ":0");
        if (config.archiveControlPort() > 0)
        {
            context.controlChannelEnabled(true)
                .controlChannel("aeron:udp?endpoint=" + config.archiveHost() + ":" + config.archiveControlPort());
        }
        else
        {
            context.controlChannelEnabled(false);
        }

        Archive archive = null;
        AeronArchive client = null;
        try
        {
            archive = launchArchive(context);
            client = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .ownsAeronClient(false)
                .controlRequestChannel(context.localControlChannel())
                .controlRequestStreamId(context.localControlStreamId())
                .controlResponseChannel(IPC)
                .controlResponseStreamId(RESPONSE_STREAMS.getAndIncrement()));
            return new EngineArchive(archive, aeron, client, archiveId, directory);
        }
        catch (final RuntimeException ex)
        {
            CloseHelper.quietCloseAll(client, archive);
            throw ex;
        }
    }

    private static Archive launchArchive(final Archive.Context context)
    {
        // After a crash the previous archive's counters stay in the media driver until its client times out.
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true)
        {
            try
            {
                return Archive.launch(context.clone());
            }
            catch (final ArchiveException ex)
            {
                if (ex.getMessage() == null || !ex.getMessage().contains("found existing archive") ||
                    System.nanoTime() - deadline > 0)
                {
                    throw ex;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
    }

    /**
     * Connect to a remote (peer) archive.
     *
     * @param aeron        local Aeron client.
     * @param host         peer address.
     * @param port         peer archive control port.
     * @param responseHost local address the peer sends responses to.
     * @return connected client (close when done).
     */
    public static AeronArchive connectRemote(final Aeron aeron, final String host, final int port,
        final String responseHost)
    {
        return AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(2))
            .controlRequestChannel("aeron:udp?endpoint=" + host + ":" + port)
            .controlResponseChannel("aeron:udp?endpoint=" + responseHost + ":0"));
    }

    /**
     * Recordings of a stream on the {@code aeron:ipc} channel, oldest first.
     *
     * @param archiveClient archive to query (local or remote).
     * @param streamId      stream.
     * @return descriptors.
     */
    public static List<RecordingInfo> recordings(final AeronArchive archiveClient, final int streamId)
    {
        final List<RecordingInfo> result = new ArrayList<>();
        archiveClient.listRecordingsForUri(0, Integer.MAX_VALUE, IPC, streamId,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, stream,
                strippedChannel, originalChannel, sourceIdentity) ->
                result.add(new RecordingInfo(recordingId, startTimestamp, startPosition,
                    stopPosition == AeronArchive.NULL_POSITION ? -1 : stopPosition, initialTermId,
                    termBufferLength, mtuLength, sessionId, stream)));
        result.sort(Comparator.comparingLong(RecordingInfo::recordingId));
        return result;
    }

    /**
     * Recordings of a stream in this archive, oldest first.
     *
     * @param streamId stream.
     * @return descriptors.
     */
    public List<RecordingInfo> recordings(final int streamId)
    {
        return recordings(client, streamId);
    }

    /**
     * Open the journal publication, extending the latest journal recording (so positions continue across restarts)
     * or starting a new one.
     *
     * @param termLength term length of a new journal.
     * @return the journal.
     */
    public Journal openJournal(final int termLength)
    {
        final List<RecordingInfo> journals = recordings(JOURNAL_STREAM_ID);
        final ExclusivePublication publication;
        final long recordingId;
        if (journals.isEmpty())
        {
            publication = aeron.addExclusivePublication(
                new ChannelUriStringBuilder(IPC).termLength(termLength).build(), JOURNAL_STREAM_ID);
            client.startRecording(sessionChannel(publication), JOURNAL_STREAM_ID, SourceLocation.LOCAL, true);
            recordingId = -1;
        }
        else
        {
            final RecordingInfo last = journals.get(journals.size() - 1);
            if (!last.isStopped())
            {
                throw new IllegalStateException("journal recording " + last.recordingId() + " is still active");
            }
            publication = aeron.addExclusivePublication(new ChannelUriStringBuilder(IPC)
                .initialPosition(last.stopPosition(), last.initialTermId(), last.termBufferLength())
                .mtu(last.mtuLength())
                .build(), JOURNAL_STREAM_ID);
            client.extendRecording(last.recordingId(), sessionChannel(publication), JOURNAL_STREAM_ID,
                SourceLocation.LOCAL, true);
            recordingId = last.recordingId();
        }
        final int counterId = awaitRecordingCounter(publication.sessionId());
        final long actualRecordingId = RecordingPos.getRecordingId(aeron.countersReader(), counterId);
        if (recordingId != -1 && recordingId != actualRecordingId)
        {
            throw new IllegalStateException("extended " + actualRecordingId + " instead of " + recordingId);
        }
        return new Journal(publication, aeron.countersReader(), counterId, actualRecordingId);
    }

    /**
     * Channel that selects one publication session for recording.
     *
     * @param publication publication.
     * @return channel URI.
     */
    static String sessionChannel(final ExclusivePublication publication)
    {
        return IPC + "?" + CommonContext.SESSION_ID_PARAM_NAME + "=" + publication.sessionId();
    }

    int awaitRecordingCounter(final int sessionId)
    {
        final CountersReader counters = aeron.countersReader();
        final long deadline = System.nanoTime() + AWAIT_TIMEOUT_NS;
        int counterId;
        while ((counterId = RecordingPos.findCounterIdBySession(counters, sessionId, client.archiveId())) ==
            CountersReader.NULL_COUNTER_ID)
        {
            if (System.nanoTime() - deadline > 0)
            {
                throw new IllegalStateException("recording did not start for session " + sessionId);
            }
            Thread.onSpinWait();
        }
        return counterId;
    }

    /**
     * Delete every journal and snapshot recording (a node that follows a new primary starts from its state).
     *
     * @return recordings purged.
     */
    public int purgeAll()
    {
        int purged = 0;
        for (final int stream : new int[] {JOURNAL_STREAM_ID, SNAPSHOT_STREAM_ID})
        {
            for (final RecordingInfo info : recordings(stream))
            {
                if (info.isStopped())
                {
                    client.purgeRecording(info.recordingId());
                    purged++;
                }
            }
        }
        return purged;
    }

    /**
     * Local archive client.
     *
     * @return client.
     */
    public AeronArchive client()
    {
        return client;
    }

    /**
     * Aeron client.
     *
     * @return client.
     */
    public Aeron aeron()
    {
        return aeron;
    }

    /**
     * Archive id.
     *
     * @return id.
     */
    public int archiveId()
    {
        return archiveId;
    }

    /**
     * Archive directory.
     *
     * @return directory.
     */
    public File archiveDirectory()
    {
        return archiveDirectory;
    }

    @Override
    public void close()
    {
        CloseHelper.closeAll(client, archive);
    }
}
