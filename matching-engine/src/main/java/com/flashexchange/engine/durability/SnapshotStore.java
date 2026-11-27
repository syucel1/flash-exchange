package com.flashexchange.engine.durability;

import com.flashexchange.engine.pipeline.OrderCommand;
import com.flashexchange.protocol.MessageHeader;
import com.flashexchange.protocol.SnapshotBeginFlyweight;
import com.flashexchange.protocol.SnapshotChunkFlyweight;
import com.flashexchange.protocol.SnapshotEndFlyweight;
import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Writes state snapshots as archive recordings (stream 110, one recording per snapshot) and reads them back.
 * A snapshot recording is {@code SnapshotBegin, SnapshotChunk*, SnapshotEnd}; the end message carries a CRC32 of
 * all payload bytes, so a torn or corrupt snapshot is detected and skipped in favour of an older one.
 */
public final class SnapshotStore
{
    private static final int CHUNK_PAYLOAD = 32 * 1024;
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    private final EngineArchive archive;
    private final int retained;
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(CHUNK_PAYLOAD + 64);
    private final SnapshotBeginFlyweight begin = new SnapshotBeginFlyweight();
    private final SnapshotChunkFlyweight chunk = new SnapshotChunkFlyweight();
    private final SnapshotEndFlyweight end = new SnapshotEndFlyweight();
    private final AtomicLong snapshotsWritten = new AtomicLong();
    private volatile long lastSnapshotSequence = -1;
    private volatile boolean closed;

    /**
     * Create a store on the local archive.
     *
     * @param archive  local archive.
     * @param retained complete snapshots to keep.
     */
    public SnapshotStore(final EngineArchive archive, final int retained)
    {
        this.archive = archive;
        this.retained = retained;
    }

    /**
     * Record a snapshot from a state snapshot slot (sections filled by the producer and the matchers).
     *
     * @param command            snapshot slot.
     * @param journalRecordingId journal the snapshot refers to.
     * @return recording id of the snapshot.
     */
    public long write(final OrderCommand command, final long journalRecordingId)
    {
        final Aeron aeron = archive.aeron();
        final AeronArchive client = archive.client();
        try (ExclusivePublication publication = aeron.addExclusivePublication(
            new ChannelUriStringBuilder(EngineArchive.IPC).termLength(4 * 1024 * 1024).build(),
            EngineArchive.SNAPSHOT_STREAM_ID))
        {
            final String channel = EngineArchive.sessionChannel(publication);
            final long subscriptionId = client.startRecording(
                channel, EngineArchive.SNAPSHOT_STREAM_ID, SourceLocation.LOCAL, true);
            try
            {
                final int counterId = archive.awaitRecordingCounter(publication.sessionId());
                final long recordingId =
                    RecordingPos.getRecordingId(aeron.countersReader(), counterId);
                final CRC32 crc = new CRC32();
                long totalBytes = 0;

                begin.wrapForEncode(buffer, 0)
                    .snapshotSequence(command.engineSequence())
                    .epoch(command.epoch())
                    .journalRecordingId(journalRecordingId)
                    .journalPosition(command.journalPosition())
                    .timestamp(command.timestamp())
                    .sectionCount(command.sectionCount());
                offer(publication, begin.encodedLength());

                for (int section = 0; section < command.sectionCount(); section++)
                {
                    final int length = command.sectionLength(section);
                    int offset = 0;
                    do
                    {
                        final int size = Math.min(CHUNK_PAYLOAD, length - offset);
                        chunk.wrapForEncode(buffer, 0).put(section, command.section(section), offset, size);
                        crc.update(buffer.byteArray(), chunk.payloadOffset(), size);
                        offer(publication, chunk.encodedLength());
                        offset += size;
                    }
                    while (offset < length);
                    totalBytes += length;
                }

                end.wrapForEncode(buffer, 0)
                    .snapshotSequence(command.engineSequence())
                    .checksum(crc.getValue())
                    .totalBytes(totalBytes);
                final long position = offer(publication, end.encodedLength());
                awaitRecorded(aeron.countersReader(), counterId, position);
                snapshotsWritten.incrementAndGet();
                lastSnapshotSequence = command.engineSequence();
                return recordingId;
            }
            finally
            {
                client.stopRecording(subscriptionId);
                purgeOld();
            }
        }
    }

    private long offer(final ExclusivePublication publication, final int length)
    {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        long result;
        while ((result = publication.offer(buffer, 0, length)) < 0)
        {
            if (closed || result == Publication.CLOSED || System.nanoTime() - deadline > 0)
            {
                throw new IllegalStateException("snapshot write failed: " + Publication.errorString(result));
            }
            Thread.onSpinWait();
        }
        return result;
    }

    private void awaitRecorded(final CountersReader counters, final int counterId, final long position)
    {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        while (counters.getCounterValue(counterId) < position)
        {
            if (closed || System.nanoTime() - deadline > 0)
            {
                throw new IllegalStateException("snapshot not recorded within timeout");
            }
            Thread.onSpinWait();
        }
    }

    private void purgeOld()
    {
        final List<RecordingInfo> snapshots = archive.recordings(EngineArchive.SNAPSHOT_STREAM_ID);
        for (int i = 0; i < snapshots.size() - retained; i++)
        {
            if (snapshots.get(i).isStopped())
            {
                archive.client().purgeRecording(snapshots.get(i).recordingId());
            }
        }
    }

    /**
     * Latest complete snapshot in an archive (local or remote).
     *
     * @param client        archive client.
     * @param aeron         local Aeron client.
     * @param replayChannel channel to replay to (IPC for local, a local UDP endpoint for a remote archive).
     * @param replayStream  replay stream id.
     * @return the snapshot or {@code null} if there is no valid one.
     */
    public static SnapshotImage latest(
        final AeronArchive client, final Aeron aeron, final String replayChannel, final int replayStream)
    {
        final List<RecordingInfo> snapshots = EngineArchive.recordings(client, EngineArchive.SNAPSHOT_STREAM_ID);
        for (int i = snapshots.size() - 1; i >= 0; i--)
        {
            final RecordingInfo info = snapshots.get(i);
            if (!info.isStopped() || info.stopPosition() == info.startPosition())
            {
                continue;
            }
            final SnapshotImage image = read(client, info, replayChannel, replayStream);
            if (image != null)
            {
                return image;
            }
        }
        return null;
    }

    static SnapshotImage read(
        final AeronArchive client, final RecordingInfo info, final String replayChannel, final int replayStream)
    {
        final Reader reader = new Reader(info.recordingId());
        final long length = info.stopPosition() - info.startPosition();
        try (Subscription subscription =
            client.replay(info.recordingId(), info.startPosition(), length, replayChannel, replayStream))
        {
            final FragmentAssembler assembler = new FragmentAssembler(
                (buffer, offset, len, header) -> reader.onMessage(buffer, offset));
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            while (!reader.done && System.nanoTime() - deadline < 0)
            {
                if (subscription.poll(assembler, 16) == 0)
                {
                    if (subscription.imageCount() > 0 && subscription.imageAtIndex(0).isEndOfStream())
                    {
                        break;
                    }
                    Thread.onSpinWait();
                }
            }
        }
        return reader.image();
    }

    /**
     * Abort any snapshot being written (engine halting).
     */
    public void close()
    {
        closed = true;
    }

    /**
     * Snapshots written by this store.
     *
     * @return count.
     */
    public long snapshotsWritten()
    {
        return snapshotsWritten.get();
    }

    /**
     * Sequence of the last snapshot written.
     *
     * @return sequence, -1 if none.
     */
    public long lastSnapshotSequence()
    {
        return lastSnapshotSequence;
    }

    private static final class Reader
    {
        private final long recordingId;
        private final SnapshotBeginFlyweight begin = new SnapshotBeginFlyweight();
        private final SnapshotChunkFlyweight chunk = new SnapshotChunkFlyweight();
        private final SnapshotEndFlyweight end = new SnapshotEndFlyweight();
        private final CRC32 crc = new CRC32();
        private ByteArrayOutputStream[] sections;
        private long snapshotSequence;
        private long epoch;
        private long journalRecordingId;
        private long journalPosition;
        private long timestamp;
        private long totalBytes;
        private boolean valid;
        private boolean done;

        Reader(final long recordingId)
        {
            this.recordingId = recordingId;
        }

        void onMessage(final DirectBuffer buffer, final int offset)
        {
            if (done || MessageHeader.schemaId(buffer, offset) != MessageHeader.SCHEMA_ID)
            {
                done = true;
                return;
            }
            switch (MessageHeader.templateId(buffer, offset))
            {
                case SnapshotBeginFlyweight.TEMPLATE_ID:
                    begin.wrapForDecode(buffer, offset);
                    snapshotSequence = begin.snapshotSequence();
                    epoch = begin.epoch();
                    journalRecordingId = begin.journalRecordingId();
                    journalPosition = begin.journalPosition();
                    timestamp = begin.timestamp();
                    sections = new ByteArrayOutputStream[begin.sectionCount()];
                    for (int i = 0; i < sections.length; i++)
                    {
                        sections[i] = new ByteArrayOutputStream();
                    }
                    break;

                case SnapshotChunkFlyweight.TEMPLATE_ID:
                    chunk.wrapForDecode(buffer, offset);
                    if (sections == null || chunk.section() < 0 || chunk.section() >= sections.length)
                    {
                        done = true;
                        return;
                    }
                    final byte[] bytes = new byte[chunk.payloadLength()];
                    buffer.getBytes(chunk.payloadOffset(), bytes);
                    crc.update(bytes);
                    sections[chunk.section()].writeBytes(bytes);
                    totalBytes += bytes.length;
                    break;

                case SnapshotEndFlyweight.TEMPLATE_ID:
                    end.wrapForDecode(buffer, offset);
                    valid = sections != null && end.snapshotSequence() == snapshotSequence &&
                        end.checksum() == crc.getValue() && end.totalBytes() == totalBytes;
                    done = true;
                    break;

                default:
                    done = true;
                    break;
            }
        }

        SnapshotImage image()
        {
            if (!valid)
            {
                return null;
            }
            final byte[][] payloads = new byte[sections.length][];
            for (int i = 0; i < sections.length; i++)
            {
                payloads[i] = sections[i].toByteArray();
            }
            return new SnapshotImage(recordingId, snapshotSequence, epoch, journalRecordingId, journalPosition,
                timestamp, payloads);
        }
    }
}
