package com.flashexchange.engine;

import com.flashexchange.engine.client.EngineClient;
import com.flashexchange.engine.client.RecordingListener;
import com.flashexchange.engine.client.RecordingListener.Report;
import com.flashexchange.engine.config.EngineChannels;
import com.flashexchange.engine.config.EngineConfig;
import com.flashexchange.engine.config.IdleStrategyType;
import com.flashexchange.engine.config.TransportMode;
import com.flashexchange.engine.config.WaitStrategyType;
import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.protocol.BookSnapshotRequestFlyweight;
import com.flashexchange.protocol.ExecType;
import com.flashexchange.protocol.OrderStatus;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.Reason;
import com.flashexchange.protocol.SelfTradePrevention;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests of the engine process over real Aeron media drivers, both over shared memory (IPC) and over
 * reliable UDP between independent media drivers (as when the engine and the trading service run on different
 * boxes).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class MatchingEngineServerIT
{
    static final int EURUSD = 1;
    static final int GBPUSD = 2;
    static final int USDJPY = 3;
    static final List<Instrument> INSTRUMENTS = List.of(
        new Instrument(EURUSD, "EURUSD", 1, 1, 1, 10_000_000, 500),
        new Instrument(GBPUSD, "GBPUSD", 1, 1, 1, 10_000_000, 500),
        new Instrument(USDJPY, "USDJPY", 1, 1, 1, 10_000_000, 0));

    @TempDir
    Path tempDir;

    final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void tearDown()
    {
        for (int i = resources.size() - 1; i >= 0; i--)
        {
            CloseHelper.quietClose(resources.get(i));
        }
    }

    <T extends AutoCloseable> T track(final T resource)
    {
        resources.add(resource);
        return resource;
    }

    EngineConfig.Builder baseConfig(final String name)
    {
        return EngineConfig.builder()
            .instruments(INSTRUMENTS)
            .aeronDirectory(tempDir.resolve(name).toString())
            .termLength(1 << 20)
            .ringBufferSize(1024)
            .orderPoolSize(1024)
            .waitStrategy(WaitStrategyType.YIELDING)
            .ingressIdleStrategy(IdleStrategyType.YIELDING)
            .snapshotIntervalMs(0)
            .shutdownTimeoutMs(2_000);
    }

    static void await(final BooleanSupplier condition, final Runnable poll, final String message)
    {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
            {
                throw new AssertionError("timed out waiting for: " + message);
            }
            poll.run();
            Thread.yield();
        }
    }

    static Runnable pollAll(final EngineClient client, final RecordingListener listener)
    {
        return () ->
        {
            client.pollExecutions(listener, 100);
            client.pollMarketData(listener, 100);
        };
    }

    Aeron connect(final String aeronDirectory)
    {
        return track(Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory)));
    }

    EngineClient client(final Aeron aeron, final EngineChannels channels)
    {
        final EngineClient client = track(new EngineClient(aeron, channels, 5_000));
        assertTrue(client.awaitConnected(10_000), "client connected");
        return client;
    }

    @Test
    void ipcEndToEndLimitMarketCancelAndSnapshot()
    {
        final MatchingEngineServer server = track(MatchingEngineServer.launch(baseConfig("ipc").build()));
        final EngineClient client = client(connect(server.aeronDirectoryName()), EngineChannels.ipc());
        final RecordingListener l = new RecordingListener();
        final Runnable poll = pollAll(client, l);

        assertTrue(client.sendLimitOrder(11, 100, EURUSD, Side.SELL, 108_345, 1_000));
        assertTrue(client.sendLimitOrder(12, 100, EURUSD, Side.SELL, 108_350, 2_000));
        await(() -> l.reports.size() == 2 && l.updates.size() == 2, poll, "two resting orders");
        final long ask1 = l.reports.get(0).orderId();
        final long ask2 = l.reports.get(1).orderId();
        assertEquals(List.of(1L, 2L), List.of(l.updates.get(0).bookSequence(), l.updates.get(1).bookSequence()));

        // market buy sweeps the first level and part of the second
        assertTrue(client.sendMarketOrder(13, 200, EURUSD, Side.BUY, 1_500));
        await(() -> l.reports.size() == 7 && l.trades.size() == 2 && l.publicTrades.size() == 2 &&
            l.updates.size() == 3, poll, "fills");
        assertEquals(108_345, l.trades.get(0).price());
        assertEquals(1_000, l.trades.get(0).quantity());
        assertEquals(108_350, l.trades.get(1).price());
        assertEquals(500, l.trades.get(1).quantity());
        assertEquals(Side.BUY, l.publicTrades.get(0).aggressor());
        assertEquals(OrderStatus.FILLED, l.reportsFor(ask1).get(1).status());
        final Report ask2Fill = l.reportsFor(ask2).get(1);
        assertEquals(OrderStatus.PARTIALLY_FILLED, ask2Fill.status());
        assertEquals(1_500, ask2Fill.leavesQuantity());
        final long takerId = l.trades.get(0).buyOrderId();
        final List<Report> taker = l.reportsFor(takerId);
        assertEquals(List.of(ExecType.NEW, ExecType.TRADE, ExecType.TRADE),
            taker.stream().map(Report::execType).toList());
        assertEquals(OrderStatus.FILLED, taker.get(2).status());

        // cancel the remainder, a foreign cancel is rejected
        assertTrue(client.sendCancel(14, 999, EURUSD, ask2));
        assertTrue(client.sendCancel(15, 100, EURUSD, ask2));
        await(() -> l.reportsFor(ask2).size() == 4, poll, "cancel");
        assertEquals(Reason.NOT_ORDER_OWNER, l.reportsFor(ask2).get(2).reason());
        assertEquals(ExecType.CANCELED, l.reportsFor(ask2).get(3).execType());
        assertEquals(Reason.USER_CANCEL, l.reportsFor(ask2).get(3).reason());

        // book is now empty: snapshot confirms it
        assertTrue(client.sendLimitOrder(16, 300, EURUSD, Side.BUY, 108_000, 700));
        assertTrue(client.requestSnapshot(1, EURUSD));
        await(() -> l.snapshots.size() == 1, poll, "snapshot");
        final RecordingListener.SnapshotRecord snapshot = l.snapshots.get(0);
        assertEquals(EURUSD, snapshot.instrumentId());
        assertEquals(l.updates.get(l.updates.size() - 1).bookSequence(), snapshot.bookSequence());
        assertEquals(1, snapshot.bids().size());
        assertEquals(108_000, snapshot.bids().get(0)[0]);
        assertEquals(700, snapshot.bids().get(0)[1]);
        assertTrue(snapshot.asks().isEmpty());

        assertTrue(client.requestSnapshot(2, BookSnapshotRequestFlyweight.ALL_INSTRUMENTS));
        await(() -> l.snapshots.size() == 4, poll, "snapshot of all instruments");
        assertEquals(0, l.unknown);
        assertEquals(0, server.pipeline().eventExceptions());
        assertEquals(0, server.ingressErrors());
        assertTrue(server.executionPublisher().published() > 0);
        assertTrue(server.marketDataPublisher().published() > 0);
        assertEquals(8, server.ingress().commandsReceived());
    }

    @Test
    void rejectsAndMalformedCommandsOverIpc()
    {
        final MatchingEngineServer server = track(MatchingEngineServer.launch(baseConfig("rejects").build()));
        final Aeron aeron = connect(server.aeronDirectoryName());
        final EngineClient client = client(aeron, EngineChannels.ipc());
        final RecordingListener l = new RecordingListener();

        client.sendNewOrder(1, 1, 99, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100, 1,
            SelfTradePrevention.NONE, false);
        client.sendNewOrder(2, 1, EURUSD, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 0, 1,
            SelfTradePrevention.NONE, false);
        await(() -> l.reports.size() == 2, pollAll(client, l), "rejects");
        assertEquals(List.of(Reason.UNKNOWN_INSTRUMENT, Reason.INVALID_PRICE),
            l.reports.stream().map(Report::reason).toList());
        assertEquals(ExecType.REJECTED, l.reports.get(0).execType());

        final Publication raw = track(aeron.addPublication(EngineChannels.IPC, EngineChannels.COMMAND_STREAM_ID));
        await(raw::isConnected, () -> { }, "raw publication");
        final UnsafeBuffer garbage = new UnsafeBuffer(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        while (raw.offer(garbage) < 0)
        {
            Thread.yield();
        }
        await(() -> server.ingress().malformedMessages() == 1, () -> { }, "malformed counted");
    }

    @Test
    void udpAcrossIndependentMediaDriversWithMultipleSubscribers()
    {
        final int base = 20_000 + ThreadLocalRandom.current().nextInt(20_000);
        final MatchingEngineServer server = track(MatchingEngineServer.launch(baseConfig("engine-box")
            .transportMode(TransportMode.UDP).udpHost("localhost").udpCommandPort(base)
            .udpExecutionControlPort(base + 1).udpMarketDataControlPort(base + 2).matcherShards(2).build()));
        assertTrue(server.config().channels().executionChannel().contains("control-mode=dynamic"));

        // two "trading service boxes", each with its own media driver
        final EngineChannels clientChannels =
            EngineChannels.udpClientSide("localhost", "localhost", base, base + 1, base + 2);
        final List<EngineClient> clients = new ArrayList<>();
        final List<RecordingListener> listeners = new ArrayList<>();
        for (int i = 0; i < 2; i++)
        {
            final MediaDriver driver = track(MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(tempDir.resolve("gateway-box-" + i).toString())
                .threadingMode(ThreadingMode.SHARED)
                .publicationTermBufferLength(1 << 20)
                .ipcTermBufferLength(1 << 20)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)));
            clients.add(client(connect(driver.aeronDirectoryName()), clientChannels));
            listeners.add(new RecordingListener());
        }

        final EngineClient seller = clients.get(0);
        final EngineClient buyer = clients.get(1);
        assertTrue(seller.sendLimitOrder(1, 10, GBPUSD, Side.SELL, 127_000, 5_000));
        assertTrue(seller.sendLimitOrder(2, 10, USDJPY, Side.SELL, 15_000, 5_000));
        // commands from different publications have no mutual ordering: wait until both asks are on the book
        final RecordingListener sellerView = listeners.get(0);
        await(() -> sellerView.updates.size() == 2, pollAll(seller, sellerView), "asks resting");
        assertTrue(buyer.sendLimitOrder(3, 20, GBPUSD, Side.BUY, 127_010, 2_000));
        assertTrue(buyer.sendMarketOrder(4, 20, USDJPY, Side.BUY, 6_000));

        for (int i = 0; i < 2; i++)
        {
            final RecordingListener l = listeners.get(i);
            await(() -> l.reports.size() == 9 && l.trades.size() == 2 && l.updates.size() == 4,
                pollAll(clients.get(i), l),
                "fan-out to subscriber " + i);
            assertEquals(127_000, l.trades.get(0).price(), "maker price");
            assertEquals(2_000, l.trades.get(0).quantity());
            assertEquals(15_000, l.trades.get(1).price());
            assertEquals(5_000, l.trades.get(1).quantity());
            final Report marketCancel = l.reports.get(l.reports.size() - 1);
            assertEquals(ExecType.CANCELED, marketCancel.execType());
            assertEquals(Reason.NO_LIQUIDITY, marketCancel.reason());
            assertEquals(5_000, marketCancel.cumQuantity());
        }
        assertEquals(0, server.executionPublisher().droppedNotConnected());
    }

    @Test
    void periodicSnapshotsArePublished()
    {
        final MatchingEngineServer server =
            track(MatchingEngineServer.launch(baseConfig("periodic").snapshotIntervalMs(50).build()));
        final EngineClient client = client(connect(server.aeronDirectoryName()), EngineChannels.ipc());
        final RecordingListener l = new RecordingListener();
        await(() -> l.snapshots.size() >= 6, pollAll(client, l), "periodic snapshots for 3 instruments");
        assertTrue(server.ingress().snapshotsScheduled() >= 6);
    }

    @Test
    void highVolumeAcrossShardsKeepsPerOrderOrdering()
    {
        final MatchingEngineServer server = track(MatchingEngineServer.launch(baseConfig("volume")
            .matcherShards(3).ringBufferSize(4096).build()));
        final EngineClient client = client(connect(server.aeronDirectoryName()), EngineChannels.ipc());
        final RecordingListener l = new RecordingListener();
        final Runnable poll = pollAll(client, l);

        final int orders = 6_000;
        for (int i = 0; i < orders; i++)
        {
            final int instrument = 1 + (i % 3);
            final Side side = (i / 3) % 2 == 0 ? Side.SELL : Side.BUY;
            assertTrue(client.sendLimitOrder(i, 1 + (i % 7), instrument, side, 1_000, 10));
            if (i % 64 == 0)
            {
                poll.run();
            }
        }
        await(() -> l.reports.size() == 2 * orders && l.publicTrades.size() == orders / 2, poll,
            "every buy matches a sell");
        assertTrue(l.trades.stream().allMatch(t -> t.quantity() == 10 && t.price() == 1_000));
        assertEquals(orders, l.reports.stream().filter(r -> r.execType() == ExecType.NEW).count());
        assertEquals(orders / 2, l.publicTrades.size());
        assertEquals(0, server.pipeline().eventExceptions());
    }

    @Test
    void outputWithoutSubscribersIsCountedAsDropped()
    {
        final MatchingEngineServer server = track(MatchingEngineServer.launch(baseConfig("nosub").build()));
        final Aeron aeron = connect(server.aeronDirectoryName());
        final Publication commands = track(aeron.addPublication(EngineChannels.IPC, EngineChannels.COMMAND_STREAM_ID));
        await(commands::isConnected, () -> { }, "command publication");
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        new com.flashexchange.protocol.NewOrderFlyweight().wrapForEncode(buffer, 0).clientOrderId(1).userId(1)
            .instrumentId(EURUSD).side(Side.BUY).orderType(OrderType.LIMIT).timeInForce(TimeInForce.GTC)
            .selfTradePrevention(SelfTradePrevention.NONE).price(100).quantity(1);
        while (commands.offer(buffer, 0, com.flashexchange.protocol.NewOrderFlyweight.ENCODED_LENGTH) < 0)
        {
            Thread.yield();
        }
        await(() -> server.executionPublisher().droppedNotConnected() == 1 &&
            server.marketDataPublisher().droppedNotConnected() == 1, () -> { }, "drops counted");
        assertEquals(0, server.executionPublisher().published());
    }

    @Test
    void connectsToExternalMediaDriver()
    {
        final MediaDriver driver = track(MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("external").toString())
            .ipcTermBufferLength(1 << 20)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)));
        final MatchingEngineServer server = track(MatchingEngineServer.launch(baseConfig("unused")
            .embeddedMediaDriver(false).aeronDirectory(driver.aeronDirectoryName()).build()));
        assertEquals(null, server.transport().mediaDriver());
        final EngineClient client = client(connect(driver.aeronDirectoryName()), EngineChannels.ipc());
        final RecordingListener l = new RecordingListener();
        client.sendLimitOrder(1, 1, EURUSD, Side.BUY, 100, 1);
        await(() -> l.reports.size() == 1, pollAll(client, l), "ack via external driver");
        server.close();
        server.close();
    }

    @Test
    void launchFailureReleasesResources()
    {
        final EngineConfig bad = baseConfig("bad").channels(new EngineChannels(
            "aeron:udp?endpoint=this-host-does-not-exist.invalid:1", 1, EngineChannels.IPC, 2, EngineChannels.IPC, 3))
            .build();
        assertThrows(RuntimeException.class, () -> MatchingEngineServer.launch(bad));
        // the same directory can be reused immediately: nothing leaked
        final MatchingEngineServer server = track(MatchingEngineServer.launch(baseConfig("bad").build()));
        assertFalse(server.aeronDirectoryName().isEmpty());
    }

    @Test
    void wiringFailureAfterTransportStartReleasesTransport()
    {
        final Instrument duplicate = INSTRUMENTS.get(0);
        final EngineConfig config = baseConfig("dup").addInstrument(duplicate).build();
        assertThrows(IllegalArgumentException.class, () -> MatchingEngineServer.launch(config));
        final MatchingEngineServer server = track(MatchingEngineServer.launch(baseConfig("dup").build()));
        server.onIngressError(new IllegalStateException("test"));
        assertEquals(1, server.ingressErrors());
    }

    @Test
    void embeddedDriverWithDefaultDirectoryAndTermLength()
    {
        final MatchingEngineServer server = track(MatchingEngineServer.launch(baseConfig("unused")
            .aeronDirectory(null).termLength(0).build()));
        assertTrue(server.transport().mediaDriver() != null);
        final EngineClient client = client(connect(server.aeronDirectoryName()), EngineChannels.ipc());
        final RecordingListener l = new RecordingListener();
        client.sendLimitOrder(1, 1, EURUSD, Side.BUY, 100, 1);
        await(() -> l.reports.size() == 1, pollAll(client, l), "ack");
    }

    @Test
    void clientWithoutEngineTimesOutAndClosedClientFailsFast()
    {
        final MediaDriver driver = track(MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("lonely").toString())
            .ipcTermBufferLength(1 << 20)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)));
        final Aeron aeron = connect(driver.aeronDirectoryName());
        final EngineClient client = new EngineClient(aeron, EngineChannels.ipc(), 50);
        assertFalse(client.isConnected());
        assertFalse(client.awaitConnected(50));
        assertFalse(client.sendLimitOrder(1, 1, EURUSD, Side.BUY, 100, 1), "not connected until timeout");
        assertFalse(client.sendNewOrder(2, 1, EURUSD, Side.BUY, OrderType.LIMIT, TimeInForce.GTC, 100, 1,
            SelfTradePrevention.NONE, true));
        client.close();
        assertFalse(client.sendCancel(3, 1, EURUSD, 1), "closed publication fails immediately");
        assertFalse(client.requestSnapshot(4, EURUSD));
    }

    @Test
    void mainEntryPointLoadsConfigFileAndStopsOnSignal() throws Exception
    {
        final Path file = tempDir.resolve("engine.properties");
        final String aeronDir = tempDir.resolve("main-file").toString().replace('\\', '/');
        java.nio.file.Files.writeString(file, String.join("\n",
            "flash.engine.instruments=1,EURUSD,1,1,1,1000,0",
            "flash.engine.aeron.dir=" + aeronDir,
            "flash.engine.aeron.termLength=1048576",
            "flash.engine.ringBufferSize=256",
            "flash.engine.orderPoolSize=64",
            "flash.engine.snapshot.intervalMs=0"));
        final Throwable[] failure = new Throwable[1];
        final Thread engineThread = new Thread(() ->
        {
            try
            {
                MatchingEngineMain.main(new String[] {file.toString()});
            }
            catch (final Throwable t)
            {
                failure[0] = t;
            }
        });
        engineThread.start();

        Aeron aeron;
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (true)
        {
            try
            {
                aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
                break;
            }
            catch (final RuntimeException ex)
            {
                if (System.nanoTime() > deadline)
                {
                    throw ex;
                }
                Thread.sleep(50);
            }
        }
        final EngineClient client = new EngineClient(aeron, EngineChannels.ipc(), 5_000);
        assertTrue(client.awaitConnected(10_000));
        final RecordingListener l = new RecordingListener();
        client.sendLimitOrder(1, 1, 1, Side.BUY, 100, 1);
        await(() -> l.reports.size() == 1, pollAll(client, l), "engine started by main(String[])");
        CloseHelper.closeAll(client, aeron);

        new ShutdownSignalBarrier().signalAll();
        engineThread.join(10_000);
        assertFalse(engineThread.isAlive());
        assertEquals(null, failure[0]);
    }

    @Test
    void mainRunsUntilSignalled() throws Exception
    {
        final EngineConfig config = baseConfig("main").build();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final Thread engineThread = new Thread(() -> MatchingEngineMain.run(config, barrier));
        engineThread.start();

        final Aeron aeron = connect(config.aeronDirectory());
        final EngineClient client = client(aeron, EngineChannels.ipc());
        final RecordingListener l = new RecordingListener();
        client.sendLimitOrder(1, 1, EURUSD, Side.BUY, 100, 1);
        await(() -> l.reports.size() == 1, pollAll(client, l), "engine started by main");

        CloseHelper.closeAll(client, aeron);
        resources.clear();
        barrier.signal();
        engineThread.join(10_000);
        assertFalse(engineThread.isAlive());
    }
}
