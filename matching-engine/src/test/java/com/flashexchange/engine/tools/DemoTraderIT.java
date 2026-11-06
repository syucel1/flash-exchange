package com.flashexchange.engine.tools;

import com.flashexchange.engine.MatchingEngineServer;
import com.flashexchange.engine.config.EngineConfig;
import com.flashexchange.engine.config.IdleStrategyType;
import com.flashexchange.engine.config.TransportMode;
import com.flashexchange.engine.config.WaitStrategyType;
import com.flashexchange.engine.matching.Instrument;
import org.agrona.CloseHelper;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 90, unit = TimeUnit.SECONDS)
class DemoTraderIT
{
    @TempDir
    Path tempDir;

    final List<AutoCloseable> resources = new ArrayList<>();
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(output, true, StandardCharsets.UTF_8);

    @AfterEach
    void tearDown()
    {
        for (int i = resources.size() - 1; i >= 0; i--)
        {
            CloseHelper.quietClose(resources.get(i));
        }
    }

    EngineConfig.Builder engine(final String dir)
    {
        return EngineConfig.builder()
            .instruments(List.of(
                new Instrument(1, "EURUSD", 1, 100, 100, 1_000_000_000L, 500),
                new Instrument(2, "GBPUSD", 1, 100, 100, 1_000_000_000L, 500)))
            .aeronDirectory(tempDir.resolve(dir).toString())
            .termLength(1 << 20)
            .ringBufferSize(4096)
            .orderPoolSize(4096)
            .waitStrategy(WaitStrategyType.YIELDING)
            .ingressIdleStrategy(IdleStrategyType.YIELDING)
            .snapshotIntervalMs(200)
            .httpPort(0);
    }

    Properties demo(final long maxOrders)
    {
        final Properties p = new Properties();
        p.setProperty("flash.demo.instruments", "1,EURUSD,108345,1,100;2,GBPUSD,127184,1,100");
        p.setProperty("flash.demo.ordersPerSecond", "4000");
        p.setProperty("flash.demo.maxOrders", String.valueOf(maxOrders));
        p.setProperty("flash.demo.reportIntervalSeconds", "1");
        p.setProperty("flash.demo.seed", "7");
        return p;
    }

    void assertHealthyTrading(final DemoTrader trader)
    {
        final DemoTrader.Stats s = trader.stats();
        assertTrue(s.ordersSent() > 0, "orders sent");
        assertTrue(s.cancelsSent() > 0, "cancels sent");
        assertEquals(0, s.sendFailures());
        assertEquals(0, s.rejected(), "the simulator only sends valid orders");
        assertEquals(s.ordersSent(), s.accepted(), "every order acknowledged");
        assertTrue(s.fills() > 0 && s.publicTrades() > 0 && s.tradedQuantity() > 0, "trading happened");
        assertTrue(s.cancelled() > 0, "cancels and IOC remainders");
        assertTrue(s.bookUpdates() > 0 && s.snapshots() > 0, "market data received");
        for (final int instrument : new int[] {1, 2})
        {
            final long bid = trader.bestBid(instrument);
            final long ask = trader.bestAsk(instrument);
            assertTrue(bid == 0 || ask == 0 || bid < ask, "local book never crossed");
        }
        assertTrue(trader.liveOrderCount() >= 0);
    }

    @Test
    void tradesAgainstEngineOverIpc()
    {
        final MatchingEngineServer server = MatchingEngineServer.launch(engine("ipc").build());
        resources.add(server);
        final Properties p = demo(3_000);
        p.setProperty("flash.demo.aeron.dir", server.aeronDirectoryName());
        final DemoTrader trader = new DemoTrader(DemoTraderConfig.fromProperties(p), out);
        resources.add(trader);
        assertTrue(trader.awaitConnected(10_000));
        trader.run(() -> true);

        assertHealthyTrading(trader);
        final String text = output.toString(StandardCharsets.UTF_8);
        assertTrue(text.contains("EURUSD") && text.contains("GBPUSD") && text.contains("trades="), text);
        assertEquals(0, server.pipeline().eventExceptions());
        assertTrue(server.ingress().commandsReceived() >= 3_000);
    }

    @Test
    void tradesAgainstEngineOverUdpWithOwnMediaDriver()
    {
        final int base = 21_000 + ThreadLocalRandom.current().nextInt(20_000);
        final MatchingEngineServer server = MatchingEngineServer.launch(engine("udp-engine")
            .transportMode(TransportMode.UDP).udpHost("localhost").udpCommandPort(base)
            .udpExecutionControlPort(base + 1).udpMarketDataControlPort(base + 2).build());
        resources.add(server);
        final Properties p = demo(1_500);
        p.setProperty("flash.demo.transport", "UDP");
        p.setProperty("flash.demo.aeron.dir", tempDir.resolve("udp-client").toString());
        p.setProperty("flash.demo.udp.commandPort", String.valueOf(base));
        p.setProperty("flash.demo.udp.executionControlPort", String.valueOf(base + 1));
        p.setProperty("flash.demo.udp.marketDataControlPort", String.valueOf(base + 2));
        final DemoTrader trader = new DemoTrader(DemoTraderConfig.fromProperties(p), out);
        resources.add(trader);
        assertTrue(trader.awaitConnected(10_000));
        trader.run(() -> true);
        assertHealthyTrading(trader);
    }

    @Test
    void stopsOnDurationOrSignalAndReportsUnreachableEngine() throws Exception
    {
        final MatchingEngineServer server = MatchingEngineServer.launch(engine("duration").build());
        resources.add(server);
        final Properties p = demo(0);
        p.setProperty("flash.demo.aeron.dir", server.aeronDirectoryName());
        p.setProperty("flash.demo.durationSeconds", "1");
        p.setProperty("flash.demo.ordersPerSecond", "200");
        assertEquals(0, DemoTrader.run(DemoTraderConfig.fromProperties(p), out, new ShutdownSignalBarrier()));
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("demo trader connected"));

        // main(String[]) with a properties file
        final Path file = tempDir.resolve("demo.properties");
        p.setProperty("flash.demo.durationSeconds", "0");
        p.setProperty("flash.demo.maxOrders", "50");
        try (var writer = Files.newBufferedWriter(file))
        {
            p.store(writer, null);
        }
        DemoTrader.main(new String[] {file.toString()});

        // UDP towards a port nobody listens on, stopped by the barrier
        final Properties unreachable = demo(10);
        unreachable.setProperty("flash.demo.transport", "UDP");
        unreachable.setProperty("flash.demo.aeron.dir", tempDir.resolve("lonely").toString());
        unreachable.setProperty("flash.demo.udp.commandPort", "1");
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final Thread signaller = new Thread(() ->
        {
            try
            {
                Thread.sleep(1_500);
            }
            catch (final InterruptedException ignore)
            {
                Thread.currentThread().interrupt();
            }
            barrier.signal();
        });
        signaller.start();
        assertEquals(2, DemoTrader.run(DemoTraderConfig.fromProperties(unreachable), out, barrier));
        signaller.join();
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("engine not reachable"));
    }
}
