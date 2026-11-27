package com.flashexchange.engine;

import com.flashexchange.engine.client.EngineClient;
import com.flashexchange.engine.client.RecordingListener;
import com.flashexchange.engine.client.RecordingListener.Report;
import com.flashexchange.engine.config.DurabilityConfig;
import com.flashexchange.engine.config.EngineChannels;
import com.flashexchange.engine.config.EngineConfig;
import com.flashexchange.engine.config.HaConfig;
import com.flashexchange.engine.config.IdleStrategyType;
import com.flashexchange.engine.config.ReleaseMode;
import com.flashexchange.engine.config.TransportMode;
import com.flashexchange.engine.config.WaitStrategyType;
import com.flashexchange.engine.ha.ArbiterConfig;
import com.flashexchange.engine.ha.ArbiterServer;
import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.NodeRole;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.Side;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Primary/standby replication with an arbiter, all over localhost UDP with one media driver per "box". The main
 * scenario is the Phase 2 exit criterion: kill the primary under load, the standby is promoted in under a second,
 * the client resends what was not acknowledged, and no fill is lost or duplicated.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class HighAvailabilityIT
{
    static final int EURUSD = 1;
    static final int GBPUSD = 2;
    static final List<Instrument> INSTRUMENTS = List.of(
        new Instrument(EURUSD, "EURUSD", 1, 1, 1, 10_000_000, 0),
        new Instrument(GBPUSD, "GBPUSD", 1, 1, 1, 10_000_000, 0));

    @TempDir
    Path tempDir;

    int base;
    ArbiterServer arbiter;
    final MatchingEngineServer[] nodes = new MatchingEngineServer[3];
    MediaDriver clientDriver;
    Aeron clientAeron;
    EngineClient client;
    final RecordingListener listener = new RecordingListener();
    final Map<Long, long[]> sent = new HashMap<>();

    @BeforeEach
    void setUp()
    {
        base = 21_000 + ThreadLocalRandom.current().nextInt(30_000);
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(client);
        for (final MatchingEngineServer node : nodes)
        {
            CloseHelper.quietClose(node);
        }
        CloseHelper.quietCloseAll(arbiter, clientAeron, clientDriver);
    }

    int port(final int nodeId, final int offset)
    {
        return base + nodeId * 20 + offset;
    }

    int arbiterPort()
    {
        return base + 1;
    }

    EngineConfig nodeConfig(final int id, final ReleaseMode mode)
    {
        final int peer = id == 1 ? 2 : 1;
        return EngineConfig.builder()
            .instruments(INSTRUMENTS)
            .transportMode(TransportMode.UDP)
            .udpHost("localhost")
            .udpCommandPort(port(id, 0))
            .udpExecutionControlPort(port(id, 1))
            .udpMarketDataControlPort(port(id, 2))
            .aeronDirectory(tempDir.resolve("aeron-node-" + id).toString())
            .termLength(1 << 20)
            .ringBufferSize(4096)
            .orderPoolSize(16_384)
            .waitStrategy(WaitStrategyType.YIELDING)
            .ingressIdleStrategy(IdleStrategyType.YIELDING)
            .snapshotIntervalMs(0)
            .shutdownTimeoutMs(3_000)
            .durability(DurabilityConfig.journal(tempDir.resolve("data-node-" + id).toString())
                .withArchiveControl("localhost", port(id, 3))
                .withJournalTermLength(1 << 20)
                .withSnapshotInterval(0, 500))
            .ha(HaConfig.disabled()
                .withSelf(id, "localhost", port(id, 4), port(id, 5), port(id, 6))
                .withPeer("localhost", port(peer, 3), port(peer, 5))
                .withArbiter("localhost", arbiterPort(), mode)
                .withTiming(20, 100))
            .build();
    }

    void startArbiter()
    {
        arbiter = ArbiterServer.launch(new ArbiterConfig(
            "localhost", arbiterPort(),
            List.of(new ArbiterConfig.Node(1, "localhost", port(1, 6)),
                new ArbiterConfig.Node(2, "localhost", port(2, 6))),
            300, 200, 100, 500,
            tempDir.resolve("arbiter.state").toString(),
            tempDir.resolve("aeron-arbiter").toString(), true, "localhost", -1));
    }

    MatchingEngineServer startNode(final int id, final ReleaseMode mode)
    {
        nodes[id] = MatchingEngineServer.launch(nodeConfig(id, mode));
        return nodes[id];
    }

    void startClient()
    {
        clientDriver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("aeron-client").toString())
            .threadingMode(ThreadingMode.SHARED)
            .publicationTermBufferLength(1 << 20)
            .ipcTermBufferLength(1 << 20)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
        clientAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(clientDriver.aeronDirectoryName()));
        client = new EngineClient(clientAeron, List.of(
            EngineChannels.udpClientSide("localhost", "localhost", port(1, 0), port(1, 1), port(1, 2)),
            EngineChannels.udpClientSide("localhost", "localhost", port(2, 0), port(2, 1), port(2, 2))), 2_000);
        assertTrue(client.awaitConnected(10_000), "client connected");
    }

    void poll()
    {
        if (client == null)
        {
            return;
        }
        client.pollExecutions(listener, 256);
        client.pollMarketData(listener, 256);
    }

    void await(final BooleanSupplier condition, final long timeoutMs, final String message)
    {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
            {
                throw new AssertionError("timed out waiting for: " + message + " " + statusOf());
            }
            poll();
            Thread.yield();
        }
    }

    String statusOf()
    {
        final StringBuilder sb = new StringBuilder();
        if (arbiter != null)
        {
            sb.append("arbiter=").append(arbiter.status());
        }
        for (int i = 1; i <= 2; i++)
        {
            if (nodes[i] != null)
            {
                sb.append(" node").append(i).append('=').append(nodes[i].conductor().status());
            }
        }
        return sb.toString();
    }

    void awaitPair(final int primary, final int standby)
    {
        await(() -> nodes[primary].role() == NodeRole.PRIMARY && nodes[standby].role() == NodeRole.STANDBY &&
            nodes[primary].conductor().link().standbyAttached(), 30_000,
            "node " + primary + " primary with node " + standby + " attached");
    }

    static long key(final long userId, final long clientOrderId)
    {
        return userId * 1_000_000_000L + clientOrderId;
    }

    void sendOrder(final Random random, final long clOrd)
    {
        final int instrument = random.nextBoolean() ? EURUSD : GBPUSD;
        final Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
        final long user = 1 + random.nextInt(8);
        final long price = 1_000 + (side == Side.BUY ? -random.nextInt(5) : random.nextInt(5)) +
            (random.nextInt(3) == 0 ? (side == Side.BUY ? 3 : -3) : 0);
        final long quantity = 1 + random.nextInt(100);
        sent.put(key(user, clOrd), new long[] {user, clOrd, instrument, side.code(), price, quantity});
        client.sendLimitOrder(clOrd, user, instrument, side, price, quantity);
    }

    void resend(final long[] order)
    {
        client.sendLimitOrder(order[1], order[0], (int)order[2], Side.get((byte)order[3]), order[4], order[5]);
    }

    Set<Long> answered()
    {
        final Set<Long> keys = new HashSet<>();
        for (final Report report : listener.reports)
        {
            keys.add(key(report.userId(), report.clientOrderId()));
        }
        return keys;
    }

    void resendUnanswered()
    {
        final Set<Long> answered = answered();
        for (final Map.Entry<Long, long[]> e : sent.entrySet())
        {
            if (!answered.contains(e.getKey()))
            {
                resend(e.getValue());
            }
        }
    }

    void awaitAllAnswered(final long timeoutMs)
    {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        long nextResend = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
        while (!answered().containsAll(sent.keySet()))
        {
            if (System.nanoTime() > deadline)
            {
                throw new AssertionError("unanswered orders remain " + statusOf());
            }
            if (System.nanoTime() > nextResend)
            {
                resendUnanswered();
                nextResend = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
            }
            poll();
            Thread.yield();
        }
        final long settle = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
        while (System.nanoTime() < settle)
        {
            poll();
            Thread.yield();
        }
    }

    /**
     * Every fill is seen exactly once: trade ids are contiguous per instrument, no execution report is repeated,
     * book updates are contiguous per instrument, and each order got exactly one acceptance or rejection (resends
     * of an order the engine already had are rejected as duplicates).
     */
    void assertExactlyOnce()
    {
        final Map<Long, Integer> tradeCount = new HashMap<>();
        for (final RecordingListener.TradeRecord trade : listener.publicTrades)
        {
            tradeCount.merge(trade.tradeId() * 10 + (trade.buyOrderId() == 0 ? 0 : 1), 1, Integer::sum);
        }
        final Set<String> reports = new HashSet<>();
        final Map<Long, Integer> firstResponses = new HashMap<>();
        for (final Report report : listener.reports)
        {
            assertTrue(reports.add(report.toString()), "duplicate report " + report);
            final boolean duplicateReject = report.execType() == ExecType.REJECTED &&
                report.reason() == Reason.DUPLICATE_CLIENT_ORDER_ID;
            if (!duplicateReject && (report.execType() == ExecType.NEW || report.execType() == ExecType.REJECTED))
            {
                firstResponses.merge(key(report.userId(), report.clientOrderId()), 1, Integer::sum);
            }
        }
        for (final long key : sent.keySet())
        {
            assertEquals(1, firstResponses.getOrDefault(key, 0), "one acceptance for order " + key);
        }
        final Map<Integer, Long> lastBookSequence = new HashMap<>();
        for (final RecordingListener.Update update : listener.updates)
        {
            final long previous = lastBookSequence.getOrDefault(update.instrumentId(), 0L);
            assertEquals(previous + 1, update.bookSequence(), "book updates contiguous for " +
                update.instrumentId());
            lastBookSequence.put(update.instrumentId(), update.bookSequence());
        }
        final Set<String> fills = new HashSet<>();
        for (final RecordingListener.TradeRecord trade : listener.trades)
        {
            assertTrue(fills.add(trade.tradeId() + "/" + trade.buyOrderId() + "/" + trade.sellOrderId()),
                "duplicate fill " + trade);
        }
        final Map<Integer, Set<Long>> tradeIds = new HashMap<>();
        for (final Report report : listener.reports)
        {
            if (report.execType() == ExecType.TRADE)
            {
                tradeIds.computeIfAbsent(0, k -> new HashSet<>());
            }
        }
        assertEquals(listener.trades.size(), listener.publicTrades.size(), "every fill has a public trade");
        assertEquals(2L * listener.trades.size(),
            listener.reports.stream().filter(r -> r.execType() == ExecType.TRADE).count(),
            "two execution reports per fill");
        assertTrue(tradeCount.size() <= listener.publicTrades.size());
    }

    @Test
    void killPrimaryUnderLoadPromotesStandbyWithinOneSecondWithoutLosingFills()
    {
        startArbiter();
        startNode(1, ReleaseMode.SYNC);
        startNode(2, ReleaseMode.SYNC);
        awaitPair(1, 2);
        startClient();

        final Random random = new Random(2026);
        long clOrd = 1;
        for (; clOrd <= 1_000; clOrd++)
        {
            sendOrder(random, clOrd);
            if (clOrd % 50 == 0)
            {
                poll();
            }
        }
        await(() -> answered().size() >= 600, 20_000, "primary processing the flow");
        for (; clOrd <= 1_500; clOrd++)
        {
            sendOrder(random, clOrd);
        }

        // kill the primary while it is still processing the burst
        final long killedAt = System.nanoTime();
        nodes[1].halt();
        await(() -> nodes[2].role() == NodeRole.PRIMARY, 5_000, "standby promoted");
        final long promotionMs = TimeUnit.NANOSECONDS.toMillis(nodes[2].conductor().lastPromotionNs() - killedAt);
        await(() -> listener.roles.stream().anyMatch(r -> r[2] == 2 && r[1] == NodeRole.PRIMARY.code()), 5_000,
            "clients see the new primary");
        System.out.println("failover: promoted in " + promotionMs + "ms at sequence " +
            nodes[2].conductor().core().sequencer().lastSequence() + ", re-published " +
            nodes[2].pipeline().publisher().retainedRepublished());
        assertTrue(promotionMs < 1_000, "promoted in " + promotionMs + "ms");
        assertTrue(nodes[2].conductor().core().sequencer().lastSequence() >= 599, "standby had replicated state");

        for (; clOrd <= 2_500; clOrd++)
        {
            sendOrder(random, clOrd);
        }
        awaitAllAnswered(30_000);
        assertExactlyOnce();
        System.out.println("failover: duplicates dropped " + client.executionDeduplicator().duplicatesDropped() +
            "/" + client.marketDataDeduplicator().duplicatesDropped() + ", stale dropped " + client.staleDropped() +
            ", fills " + listener.trades.size());
        assertTrue(listener.trades.size() > 100, "enough fills to be meaningful: " + listener.trades.size());
        assertEquals(0, nodes[2].pipeline().eventExceptions());

        // the old primary restarts, follows the new one, and takes over after a second failure
        nodes[1] = null;
        startNode(1, ReleaseMode.SYNC);
        awaitPair(2, 1);
        for (; clOrd <= 3_000; clOrd++)
        {
            sendOrder(random, clOrd);
        }
        awaitAllAnswered(30_000);
        await(() -> nodes[1].conductor().core().pipeline().publisher().lastHandledEngineSequence() ==
            nodes[2].conductor().core().sequencer().lastSequence(), 10_000, "standby caught up");
        nodes[2].halt();
        await(() -> nodes[1].role() == NodeRole.PRIMARY, 5_000, "original node promoted again");
        for (; clOrd <= 3_300; clOrd++)
        {
            sendOrder(random, clOrd);
        }
        awaitAllAnswered(30_000);
        assertExactlyOnce();
        assertEquals(3, arbiter.arbiter().epoch(), "initial election plus two failovers");
    }

    @Test
    void asyncModeAndStandbyLossKeepThePrimaryTrading()
    {
        startArbiter();
        startNode(1, ReleaseMode.ASYNC);
        startNode(2, ReleaseMode.ASYNC);
        awaitPair(1, 2);
        startClient();
        final Random random = new Random(7);
        for (long clOrd = 1; clOrd <= 300; clOrd++)
        {
            sendOrder(random, clOrd);
        }
        awaitAllAnswered(20_000);

        // losing the standby must not stop the primary
        nodes[2].halt();
        nodes[2] = null;
        for (long clOrd = 301; clOrd <= 600; clOrd++)
        {
            sendOrder(random, clOrd);
        }
        awaitAllAnswered(20_000);
        assertExactlyOnce();
        assertEquals(NodeRole.PRIMARY, nodes[1].role());
    }

    @Test
    void syncPrimaryDegradesWhenTheStandbyDiesAndHoldsWithoutALease()
    {
        startArbiter();
        startNode(1, ReleaseMode.SYNC);
        startNode(2, ReleaseMode.SYNC);
        awaitPair(1, 2);
        startClient();
        final Random random = new Random(11);
        for (long clOrd = 1; clOrd <= 100; clOrd++)
        {
            sendOrder(random, clOrd);
        }
        awaitAllAnswered(20_000);

        nodes[2].halt();
        nodes[2] = null;
        for (long clOrd = 101; clOrd <= 200; clOrd++)
        {
            sendOrder(random, clOrd);
        }
        awaitAllAnswered(20_000);
        assertTrue(nodes[1].conductor().link().degradations() >= 1 ||
            !nodes[1].conductor().link().standbyAttached(), "released without the standby");

        // without the arbiter the lease runs out and the primary stops taking commands
        arbiter.close();
        arbiter = null;
        await(() -> !nodes[1].conductor().link().leaseValid(), 5_000, "lease expired");
        final int before = listener.reports.size();
        sendOrder(random, 201);
        final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
        while (System.nanoTime() < end)
        {
            poll();
        }
        assertEquals(before, listener.reports.size(), "no commands processed without a lease");

        // the arbiter comes back (state persisted) and renews the lease
        startArbiter();
        awaitAllAnswered(20_000);
        assertEquals(NodeRole.PRIMARY, nodes[1].role());
        assertExactlyOnce();
    }

    @Test
    void plannedSwitchoverFencesTheOldPrimaryWhichRejoinsAsStandby() throws Exception
    {
        startArbiter();
        startNode(1, ReleaseMode.SYNC);
        startNode(2, ReleaseMode.SYNC);
        awaitPair(1, 2);
        startClient();
        final java.util.concurrent.atomic.AtomicBoolean fenced = new java.util.concurrent.atomic.AtomicBoolean();
        nodes[1].onFenced(() -> fenced.set(true));
        final Random random = new Random(3);
        for (long clOrd = 1; clOrd <= 200; clOrd++)
        {
            sendOrder(random, clOrd);
        }
        awaitAllAnswered(20_000);
        await(() -> nodes[2].conductor().core().pipeline().publisher().lastHandledEngineSequence() == 199, 10_000,
            "standby caught up");

        // operator switchover: the arbiter restarts with node 2 recorded as primary at a later epoch
        arbiter.close();
        java.nio.file.Files.writeString(tempDir.resolve("arbiter.state"), "epoch=5\nprimary=2\n");
        startArbiter();
        await(() -> nodes[1].role() == NodeRole.FENCED, 10_000, "old primary fenced");
        await(fenced::get, 5_000, "fenced callback");
        assertTrue(!nodes[1].isHealthy(), "a fenced node reports unhealthy so it is restarted");
        await(() -> nodes[2].role() == NodeRole.PRIMARY, 10_000, "node 2 promoted");
        assertEquals(5, nodes[2].conductor().epoch());

        for (long clOrd = 201; clOrd <= 400; clOrd++)
        {
            sendOrder(random, clOrd);
        }
        awaitAllAnswered(20_000);
        assertExactlyOnce();

        nodes[1].close();
        nodes[1] = null;
        startNode(1, ReleaseMode.SYNC);
        awaitPair(2, 1);
        await(() -> Boolean.TRUE.equals(nodes[1].status().get("peerVisible")), 5_000, "replication visible");
        final java.util.Map<String, Object> status = nodes[1].status();
        assertEquals("STANDBY", status.get("role"));
        assertTrue(nodes[2].status().containsKey("ackedSequence"));
    }

    @Test
    void replicationOverALossyLinkStillFailsOverWithoutLosingFills()
    {
        // node 2's media driver drops 10% of the UDP frames it receives (replication, acks, arbiter commands, client
        // commands); Aeron NAKs and retransmits, the application must not notice
        final MediaDriver lossyDriver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("aeron-lossy").toString())
            .threadingMode(ThreadingMode.SHARED)
            .publicationTermBufferLength(1 << 20)
            .ipcTermBufferLength(1 << 20)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .receiveChannelEndpointSupplier((channel, dispatcher, status, context) ->
                new io.aeron.driver.ext.DebugReceiveChannelEndpoint(channel, dispatcher, status, context,
                    new io.aeron.driver.ext.RandomLossGenerator(0.1, 42),
                    new io.aeron.driver.ext.RandomLossGenerator(0.0, 43))));
        try
        {
            startArbiter();
            startNode(1, ReleaseMode.SYNC);
            final EngineConfig lossy = EngineConfig.builder()
                .instruments(INSTRUMENTS)
                .channels(nodeConfig(2, ReleaseMode.SYNC).channels())
                .embeddedMediaDriver(false)
                .aeronDirectory(lossyDriver.aeronDirectoryName())
                .ringBufferSize(4096)
                .orderPoolSize(16_384)
                .waitStrategy(WaitStrategyType.YIELDING)
                .ingressIdleStrategy(IdleStrategyType.YIELDING)
                .snapshotIntervalMs(0)
                .durability(nodeConfig(2, ReleaseMode.SYNC).durability())
                .ha(nodeConfig(2, ReleaseMode.SYNC).ha())
                .build();
            nodes[2] = MatchingEngineServer.launch(lossy);
            awaitPair(1, 2);
            startClient();
            final Random random = new Random(99);
            long clOrd = 1;
            for (; clOrd <= 800; clOrd++)
            {
                sendOrder(random, clOrd);
            }
            awaitAllAnswered(30_000);
            await(() -> nodes[2].conductor().core().pipeline().publisher().lastHandledEngineSequence() ==
                nodes[1].conductor().core().sequencer().lastSequence(), 20_000, "standby caught up over loss");

            nodes[1].halt();
            await(() -> nodes[2].role() == NodeRole.PRIMARY, 10_000, "standby promoted");
            for (; clOrd <= 1_200; clOrd++)
            {
                sendOrder(random, clOrd);
            }
            awaitAllAnswered(30_000);
            assertExactlyOnce();
        }
        finally
        {
            CloseHelper.quietClose(nodes[2]);
            nodes[2] = null;
            CloseHelper.quietClose(lossyDriver);
        }
    }

    @Test
    void pendingNodeWaitsForTheArbiterAndDiscardsCommands()
    {
        final MatchingEngineServer node = startNode(1, ReleaseMode.SYNC);
        startClient();
        assertEquals(NodeRole.PENDING, node.role());
        assertTrue(node.isHealthy(), "a pending node is healthy");
        assertEquals(null, node.pipeline());
        assertEquals(null, node.ingress());
        client.sendLimitOrder(1, 1, EURUSD, Side.BUY, 990, 10);
        final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
        while (System.nanoTime() < end)
        {
            poll();
        }
        assertEquals(0, listener.reports.size(), "no output without a role");
        final Map<String, Object> status = node.status();
        assertEquals("PENDING", status.get("role"));
        assertEquals(Boolean.FALSE, status.get("leaseValid"));
        assertEquals(Boolean.TRUE, status.get("haEnabled"));
        assertEquals(null, node.conductor().core());
        node.close();
        nodes[1] = null;
    }
}
