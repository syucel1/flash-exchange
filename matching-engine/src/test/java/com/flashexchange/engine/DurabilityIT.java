package com.flashexchange.engine;

import com.flashexchange.engine.client.EngineClient;
import com.flashexchange.engine.client.RecordingListener;
import com.flashexchange.engine.config.DurabilityConfig;
import com.flashexchange.engine.config.EngineChannels;
import com.flashexchange.engine.config.EngineConfig;
import com.flashexchange.engine.config.IdleStrategyType;
import com.flashexchange.engine.config.WaitStrategyType;
import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.engine.pipeline.EnginePipeline;
import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.NodeRole;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.Side;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Journal, snapshots and cold recovery of a standalone engine. The media driver runs outside the engine so the
 * client survives engine restarts, as it would in production.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class DurabilityIT
{
    static final int EURUSD = 1;
    static final int GBPUSD = 2;
    static final List<Instrument> INSTRUMENTS = List.of(
        new Instrument(EURUSD, "EURUSD", 1, 1, 1, 10_000_000, 0),
        new Instrument(GBPUSD, "GBPUSD", 1, 1, 1, 10_000_000, 0));

    @TempDir
    Path tempDir;

    MediaDriver driver;
    Aeron aeron;
    EngineClient client;
    RecordingListener listener;
    MatchingEngineServer server;
    final List<AutoCloseable> resources = new ArrayList<>();

    @BeforeEach
    void setUp()
    {
        driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("aeron").toString())
            .threadingMode(ThreadingMode.SHARED)
            .ipcTermBufferLength(1 << 20)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(server);
        for (int i = resources.size() - 1; i >= 0; i--)
        {
            CloseHelper.quietClose(resources.get(i));
        }
        CloseHelper.quietCloseAll(client, aeron, driver);
    }

    EngineConfig config(final DurabilityConfig durability)
    {
        return EngineConfig.builder()
            .instruments(INSTRUMENTS)
            .embeddedMediaDriver(false)
            .aeronDirectory(driver.aeronDirectoryName())
            .ringBufferSize(1024)
            .orderPoolSize(4096)
            .waitStrategy(WaitStrategyType.YIELDING)
            .ingressIdleStrategy(IdleStrategyType.YIELDING)
            .snapshotIntervalMs(0)
            .shutdownTimeoutMs(5_000)
            .durability(durability)
            .build();
    }

    DurabilityConfig journal()
    {
        return DurabilityConfig.journal(tempDir.resolve("data").toString()).withSnapshotInterval(0, 0);
    }

    MatchingEngineServer start(final DurabilityConfig durability)
    {
        server = MatchingEngineServer.launch(config(durability));
        if (client == null)
        {
            client = new EngineClient(aeron, EngineChannels.ipc(), 5_000);
            listener = new RecordingListener();
        }
        assertTrue(client.awaitConnected(10_000), "client connected");
        return server;
    }

    void poll()
    {
        client.pollExecutions(listener, 256);
        client.pollMarketData(listener, 256);
    }

    void await(final BooleanSupplier condition, final String message)
    {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
            {
                throw new AssertionError("timed out waiting for: " + message);
            }
            poll();
            Thread.yield();
        }
    }

    void settle()
    {
        final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
        while (System.nanoTime() < end)
        {
            poll();
            Thread.yield();
        }
    }

    /** Deterministic order flow: limits around a mid price, some market orders, some cancels. */
    static void sendFlow(final EngineClient client, final long seed, final int count, final long clOrdBase)
    {
        final Random random = new Random(seed);
        for (int i = 0; i < count; i++)
        {
            final int instrument = random.nextBoolean() ? EURUSD : GBPUSD;
            final Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
            final long user = 1 + random.nextInt(5);
            final long clOrd = clOrdBase + i;
            final int kind = random.nextInt(10);
            final boolean sent;
            if (kind < 7)
            {
                final long price = 1_000 + (side == Side.BUY ? -random.nextInt(6) : random.nextInt(6)) +
                    (random.nextInt(4) == 0 ? (side == Side.BUY ? 4 : -4) : 0);
                sent = client.sendLimitOrder(clOrd, user, instrument, side, price, 1 + random.nextInt(100));
            }
            else if (kind < 9)
            {
                sent = client.sendMarketOrder(clOrd, user, instrument, side, 1 + random.nextInt(50));
            }
            else
            {
                sent = client.sendCancel(clOrd, user, instrument, 1 + random.nextInt(Math.max(1, i)));
            }
            assertTrue(sent, "sent " + i);
        }
    }

    static byte[] state(final MatchingEngineServer server)
    {
        final EnginePipeline pipeline = server.pipeline();
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1024);
        final int length = pipeline.engine(0).writeState(buffer, 0);
        return Arrays.copyOf(buffer.byteArray(), length);
    }

    void awaitHandled(final MatchingEngineServer engine, final long sequence)
    {
        await(() -> engine.pipeline().publisher().lastHandledEngineSequence() == sequence,
            "engine handled sequence " + sequence);
    }

    @Test
    void crashRecoveryReplaysTheJournalToTheSameState()
    {
        start(journal());
        sendFlow(client, 42, 400, 1_000);
        await(() -> client.executionDeduplicator().lastSequence() == 399, "all commands processed");
        settle();
        final byte[] before = state(server);
        final int reportsBefore = listener.reports.size();
        final int tradesBefore = listener.trades.size();
        assertEquals(NodeRole.STANDALONE, server.role());
        assertEquals(1, server.conductor().epoch());

        server.halt();
        start(journal());
        awaitHandled(server, 399);
        settle();

        assertEquals(400, server.conductor().core().journalEntriesApplied(), "whole journal replayed");
        assertArrayEquals(before, state(server), "recovered book state");
        assertEquals(399, server.conductor().core().sequencer().lastSequence());
        assertEquals(2, server.conductor().epoch());
        assertEquals(reportsBefore, listener.reports.size(), "re-published output is de-duplicated");
        assertEquals(tradesBefore, listener.trades.size());
        assertTrue(client.executionDeduplicator().duplicatesDropped() > 0, "output was re-published");
        assertTrue(listener.roles.stream().anyMatch(r -> r[0] == 2), "new epoch announced");

        // sequencing continues where it left off
        assertTrue(client.sendLimitOrder(5_000, 9, EURUSD, Side.BUY, 1, 10));
        await(() -> listener.reports.stream().anyMatch(r -> r.clientOrderId() == 5_000), "new order accepted");
        assertEquals(401, listener.reports.stream().filter(r -> r.clientOrderId() == 5_000).findFirst()
            .orElseThrow().orderId());
        assertEquals(0, server.pipeline().eventExceptions());
    }

    @Test
    void gracefulRestartRestoresFromTheShutdownSnapshot()
    {
        start(journal());
        sendFlow(client, 7, 250, 1_000);
        await(() -> client.executionDeduplicator().lastSequence() == 249, "all commands processed");
        settle();
        final byte[] before = state(server);
        server.close();

        start(journal());
        settle();
        assertEquals(0, server.conductor().core().journalEntriesApplied(), "snapshot covers the whole journal");
        assertArrayEquals(before, state(server));
        assertEquals(249, server.conductor().core().sequencer().lastSequence());
    }

    @Test
    void recoversFromSnapshotPlusJournalTail()
    {
        final DurabilityConfig durability = journal().withSnapshotInterval(0, 50);
        start(durability);
        sendFlow(client, 99, 175, 1_000);
        await(() -> client.executionDeduplicator().lastSequence() == 174, "all commands processed");
        settle();
        final byte[] before = state(server);
        assertTrue(server.conductor().core().snapshotStore().snapshotsWritten() >= 3, "periodic snapshots");

        server.halt();
        start(durability);
        awaitHandled(server, 174);
        settle();
        final long replayed = server.conductor().core().journalEntriesApplied();
        assertTrue(replayed > 0 && replayed < 175, "replayed only the tail: " + replayed);
        assertArrayEquals(before, state(server));
    }

    @Test
    void duplicateClientOrderIdIsRejectedAcrossRestarts()
    {
        start(journal());
        assertTrue(client.sendLimitOrder(77, 1, EURUSD, Side.BUY, 990, 10));
        assertTrue(client.sendLimitOrder(77, 1, EURUSD, Side.BUY, 990, 10));
        assertTrue(client.sendLimitOrder(77, 2, EURUSD, Side.BUY, 990, 10));
        await(() -> listener.reports.size() == 3, "three reports");
        assertEquals(ExecType.NEW, listener.reports.get(0).execType());
        assertEquals(ExecType.REJECTED, listener.reports.get(1).execType());
        assertEquals(Reason.DUPLICATE_CLIENT_ORDER_ID, listener.reports.get(1).reason());
        assertEquals(ExecType.NEW, listener.reports.get(2).execType(), "same id, other user");

        server.halt();
        start(journal());
        awaitHandled(server, 2);
        assertTrue(client.sendLimitOrder(77, 1, EURUSD, Side.BUY, 990, 10));
        await(() -> listener.reports.size() == 4, "fourth report");
        assertEquals(Reason.DUPLICATE_CLIENT_ORDER_ID, listener.reports.get(3).reason());
        assertEquals(1, server.conductor().core().ingress().duplicatesDetected());
    }

    @Test
    void inMemoryEngineStillWorksWithoutAJournal()
    {
        start(DurabilityConfig.disabled());
        assertTrue(client.sendLimitOrder(1, 1, EURUSD, Side.BUY, 990, 10));
        await(() -> listener.reports.size() == 1, "report");
        assertEquals(0, listener.roles.size(), "no role announcement without durability");
        assertTrue(server.status().containsKey("role"));
    }

    @Test
    void corruptOrIncompleteSnapshotsAreSkipped()
    {
        start(journal());
        sendFlow(client, 5, 120, 1_000);
        await(() -> client.executionDeduplicator().lastSequence() == 119, "all commands processed");
        settle();
        final byte[] before = state(server);
        server.close();
        server = null;

        // append two bogus snapshot recordings: a bad checksum, and garbage
        try (com.flashexchange.engine.durability.EngineArchive archive =
            com.flashexchange.engine.durability.EngineArchive.launch(journal(), 1, driver.aeronDirectoryName(), aeron))
        {
            writeBogusSnapshot(archive, true);
            writeBogusSnapshot(archive, false);
            assertEquals(4, archive.recordings(com.flashexchange.engine.durability.EngineArchive.SNAPSHOT_STREAM_ID)
                .size(), "two real and two bogus snapshots");
        }

        start(journal());
        settle();
        assertArrayEquals(before, state(server), "restored from the last valid snapshot");
        assertEquals(119, server.conductor().core().sequencer().lastSequence());
    }

    void writeBogusSnapshot(final com.flashexchange.engine.durability.EngineArchive archive, final boolean badCrc)
    {
        final io.aeron.ExclusivePublication publication = aeron.addExclusivePublication("aeron:ipc",
            com.flashexchange.engine.durability.EngineArchive.SNAPSHOT_STREAM_ID);
        final long subscriptionId = archive.client().startRecording("aeron:ipc?session-id=" +
            publication.sessionId(), com.flashexchange.engine.durability.EngineArchive.SNAPSHOT_STREAM_ID,
            io.aeron.archive.codecs.SourceLocation.LOCAL, true);
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(256);
        while (!publication.isConnected())
        {
            Thread.yield();
        }
        final int length;
        if (badCrc)
        {
            new com.flashexchange.protocol.SnapshotBeginFlyweight().wrapForEncode(buffer, 0).snapshotSequence(1_000)
                .sectionCount(2);
            offer(publication, buffer, com.flashexchange.protocol.SnapshotBeginFlyweight.ENCODED_LENGTH);
            final com.flashexchange.protocol.SnapshotChunkFlyweight chunk =
                new com.flashexchange.protocol.SnapshotChunkFlyweight().wrapForEncode(buffer, 0)
                    .put(0, new org.agrona.concurrent.UnsafeBuffer(new byte[16]), 0, 16);
            offer(publication, buffer, chunk.encodedLength());
            new com.flashexchange.protocol.SnapshotEndFlyweight().wrapForEncode(buffer, 0).snapshotSequence(1_000)
                .checksum(12345).totalBytes(16);
            length = com.flashexchange.protocol.SnapshotEndFlyweight.ENCODED_LENGTH;
        }
        else
        {
            buffer.putLong(0, 42);
            length = 16;
        }
        final long position = offer(publication, buffer, length);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (archive.recordings(com.flashexchange.engine.durability.EngineArchive.SNAPSHOT_STREAM_ID).stream()
            .noneMatch(r -> r.sessionId() == publication.sessionId()) || recorded(archive, publication) < position)
        {
            assertTrue(System.nanoTime() < deadline, "bogus snapshot recorded");
            Thread.yield();
        }
        archive.client().stopRecording(subscriptionId);
        publication.close();
    }

    static long recorded(final com.flashexchange.engine.durability.EngineArchive archive,
        final io.aeron.ExclusivePublication publication)
    {
        final int counterId = io.aeron.archive.status.RecordingPos.findCounterIdBySession(
            archive.aeron().countersReader(), publication.sessionId(), archive.client().archiveId());
        return counterId < 0 ? -1 : archive.aeron().countersReader().getCounterValue(counterId);
    }

    static long offer(final io.aeron.ExclusivePublication publication, final ExpandableArrayBuffer buffer,
        final int length)
    {
        long result;
        while ((result = publication.offer(buffer, 0, length)) < 0)
        {
            Thread.yield();
        }
        return result;
    }

    @Test
    void changingTheShardCountOfARecoveredEngineIsRejected()
    {
        start(journal());
        assertTrue(client.sendLimitOrder(1, 1, EURUSD, Side.BUY, 990, 10));
        await(() -> listener.reports.size() == 1, "report");
        server.close();
        server = null;
        final EngineConfig twoShards = EngineConfig.builder()
            .instruments(INSTRUMENTS)
            .embeddedMediaDriver(false)
            .aeronDirectory(driver.aeronDirectoryName())
            .ringBufferSize(1024)
            .orderPoolSize(1024)
            .matcherShards(2)
            .durability(journal())
            .build();
        final IllegalStateException ex =
            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> MatchingEngineServer.launch(twoShards));
        assertTrue(ex.getMessage().contains("matcher shards"), ex.getMessage());
    }
}
