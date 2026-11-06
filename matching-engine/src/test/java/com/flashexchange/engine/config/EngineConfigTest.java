package com.flashexchange.engine.config;

import com.flashexchange.engine.matching.Instrument;
import io.aeron.driver.ThreadingMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EngineConfigTest
{
    static final Instrument EURUSD = new Instrument(1, "EURUSD", 1, 100, 100, 1_000_000_00, 500);

    @Test
    void builderDefaults()
    {
        final EngineConfig config = EngineConfig.builder().addInstrument(EURUSD).build();
        assertEquals(TransportMode.IPC, config.transportMode());
        assertEquals(EngineChannels.ipc(), config.channels());
        assertNull(config.aeronDirectory());
        assertTrue(config.embeddedMediaDriver());
        assertEquals(ThreadingMode.SHARED, config.driverThreadingMode());
        assertTrue(config.deleteAeronDirectoryOnStart());
        assertEquals(0, config.termLength());
        assertEquals(List.of(EURUSD), config.instruments());
        assertEquals(65536, config.ringBufferSize());
        assertEquals(WaitStrategyType.SLEEPING, config.waitStrategy());
        assertEquals(IdleStrategyType.BACKOFF, config.ingressIdleStrategy());
        assertEquals(1, config.matcherShards());
        assertEquals(256, config.fragmentLimit());
        assertEquals(65536, config.orderPoolSize());
        assertEquals(1000, config.snapshotIntervalMs());
        assertEquals(0, config.snapshotDepth());
        assertEquals(1000, config.publishRetryTimeoutMs());
        assertEquals(5000, config.shutdownTimeoutMs());
        assertEquals("0.0.0.0", config.httpHost());
        assertEquals(-1, config.httpPort());
        assertTrue(config.toString().contains("transport=IPC"));
    }

    @Test
    void udpModeDerivesEngineSideChannels()
    {
        final EngineConfig config = EngineConfig.builder().addInstrument(EURUSD)
            .transportMode(TransportMode.UDP).udpHost("10.0.0.5").udpCommandPort(1).udpExecutionControlPort(2)
            .udpMarketDataControlPort(3).build();
        final EngineChannels c = config.channels();
        assertEquals("aeron:udp?endpoint=10.0.0.5:1", c.commandChannel());
        assertEquals("aeron:udp?control=10.0.0.5:2|control-mode=dynamic", c.executionChannel());
        assertEquals("aeron:udp?control=10.0.0.5:3|control-mode=dynamic", c.marketDataChannel());
        assertEquals(1001, c.commandStreamId());
        assertEquals(2001, c.executionStreamId());
        assertEquals(3001, c.marketDataStreamId());
    }

    @Test
    void clientSideUdpChannelsMatchEngineSide()
    {
        final EngineChannels c = EngineChannels.udpClientSide("engine", "client", 1, 2, 3);
        assertEquals("aeron:udp?endpoint=engine:1", c.commandChannel());
        assertEquals("aeron:udp?endpoint=client:0|control=engine:2|control-mode=dynamic", c.executionChannel());
        assertEquals("aeron:udp?endpoint=client:0|control=engine:3|control-mode=dynamic", c.marketDataChannel());
        assertThrows(NullPointerException.class, () -> new EngineChannels(null, 1, "a", 2, "b", 3));
        assertThrows(NullPointerException.class, () -> new EngineChannels("a", 1, null, 2, "b", 3));
        assertThrows(NullPointerException.class, () -> new EngineChannels("a", 1, "b", 2, null, 3));
    }

    @Test
    void fromPropertiesReadsEveryKey()
    {
        final Properties p = new Properties();
        p.setProperty("flash.engine.transport", "udp");
        p.setProperty("flash.engine.udp.host", "engine-host");
        p.setProperty("flash.engine.udp.commandPort", "5001");
        p.setProperty("flash.engine.udp.executionControlPort", "5002");
        p.setProperty("flash.engine.udp.marketDataControlPort", "5003");
        p.setProperty("flash.engine.channel.marketData", "aeron:udp?endpoint=239.1.1.1:5004|interface=0.0.0.0");
        p.setProperty("flash.engine.stream.command", "11");
        p.setProperty("flash.engine.stream.execution", "12");
        p.setProperty("flash.engine.stream.marketData", "13");
        p.setProperty("flash.engine.aeron.dir", "/dev/shm/x");
        p.setProperty("flash.engine.aeron.embeddedMediaDriver", "false");
        p.setProperty("flash.engine.aeron.driverThreadingMode", "dedicated");
        p.setProperty("flash.engine.aeron.deleteDirOnStart", "false");
        p.setProperty("flash.engine.aeron.termLength", "1048576");
        p.setProperty("flash.engine.ringBufferSize", "1024");
        p.setProperty("flash.engine.waitStrategy", "busy_spin");
        p.setProperty("flash.engine.ingressIdleStrategy", "busy_spin");
        p.setProperty("flash.engine.matcherShards", "4");
        p.setProperty("flash.engine.fragmentLimit", "10");
        p.setProperty("flash.engine.orderPoolSize", "100");
        p.setProperty("flash.engine.snapshot.intervalMs", "0");
        p.setProperty("flash.engine.snapshot.depth", "20");
        p.setProperty("flash.engine.publish.retryTimeoutMs", "5");
        p.setProperty("flash.engine.shutdownTimeoutMs", "6");
        p.setProperty("flash.engine.http.host", "127.0.0.1");
        p.setProperty("flash.engine.http.port", "8080");
        p.setProperty("flash.engine.instruments", "1,EURUSD,1,100,100,10000000,500; 2,GBPUSD,1,100,100,10000000,0;");

        final EngineConfig c = EngineConfig.fromProperties(p);
        assertEquals(TransportMode.UDP, c.transportMode());
        assertEquals("aeron:udp?endpoint=engine-host:5001", c.channels().commandChannel());
        assertEquals("aeron:udp?control=engine-host:5002|control-mode=dynamic", c.channels().executionChannel());
        assertEquals("aeron:udp?endpoint=239.1.1.1:5004|interface=0.0.0.0", c.channels().marketDataChannel());
        assertEquals(11, c.channels().commandStreamId());
        assertEquals(12, c.channels().executionStreamId());
        assertEquals(13, c.channels().marketDataStreamId());
        assertEquals("/dev/shm/x", c.aeronDirectory());
        assertFalse(c.embeddedMediaDriver());
        assertEquals(ThreadingMode.DEDICATED, c.driverThreadingMode());
        assertFalse(c.deleteAeronDirectoryOnStart());
        assertEquals(1048576, c.termLength());
        assertEquals(1024, c.ringBufferSize());
        assertEquals(WaitStrategyType.BUSY_SPIN, c.waitStrategy());
        assertEquals(IdleStrategyType.BUSY_SPIN, c.ingressIdleStrategy());
        assertEquals(4, c.matcherShards());
        assertEquals(10, c.fragmentLimit());
        assertEquals(100, c.orderPoolSize());
        assertEquals(0, c.snapshotIntervalMs());
        assertEquals(20, c.snapshotDepth());
        assertEquals(5, c.publishRetryTimeoutMs());
        assertEquals(6, c.shutdownTimeoutMs());
        assertEquals("127.0.0.1", c.httpHost());
        assertEquals(8080, c.httpPort());
        assertEquals(2, c.instruments().size());
        assertEquals("GBPUSD", c.instruments().get(1).symbol());
        assertEquals(0, c.instruments().get(1).collarBps());
    }

    @Test
    void fromPropertiesWithOnlyInstrumentsUsesDefaults()
    {
        final Properties p = new Properties();
        p.setProperty("flash.engine.instruments", "1,EURUSD,1,1,1,10,0");
        p.setProperty("flash.engine.matcherShards", " ");
        p.setProperty("flash.engine.channel.command", "aeron:ipc?alias=cmd");
        p.setProperty("flash.engine.channel.execution", "aeron:ipc?alias=exec");
        final EngineConfig c = EngineConfig.fromProperties(p);
        assertEquals(1, c.matcherShards());
        assertEquals("aeron:ipc?alias=cmd", c.channels().commandChannel());
        assertEquals("aeron:ipc?alias=exec", c.channels().executionChannel());
        assertEquals("aeron:ipc", c.channels().marketDataChannel());
        assertThrows(IllegalArgumentException.class, () -> EngineConfig.fromProperties(new Properties()),
            "no instruments");
    }

    @Test
    void loadMergesFileAndSystemProperties(@TempDir final Path dir) throws IOException
    {
        final Path file = dir.resolve("engine.properties");
        Files.writeString(file, "flash.engine.instruments=1,EURUSD,1,1,1,10,0\nflash.engine.matcherShards=2\n");
        System.setProperty("flash.engine.matcherShards", "3");
        System.setProperty("unrelated.key", "x");
        try
        {
            assertEquals(3, EngineConfig.load(file).matcherShards());
            System.setProperty("flash.engine.instruments", "5,X,1,1,1,10,0");
            assertEquals(5, EngineConfig.load(null).instruments().get(0).id());
        }
        finally
        {
            System.clearProperty("flash.engine.matcherShards");
            System.clearProperty("flash.engine.instruments");
            System.clearProperty("unrelated.key");
        }
    }

    @Test
    void parseInstrumentsRejectsMalformedEntries()
    {
        assertEquals(1, EngineConfig.parseInstruments(" ; 1,A,1,1,1,1,0 ;").size());
        assertThrows(IllegalArgumentException.class, () -> EngineConfig.parseInstruments("1,A,1,1,1,1"));
        assertThrows(NumberFormatException.class, () -> EngineConfig.parseInstruments("x,A,1,1,1,1,0"));
    }

    @Test
    void validationRejectsBadValues()
    {
        assertThrows(IllegalArgumentException.class, () -> EngineConfig.builder().build());
        assertInvalid(EngineConfig.builder().ringBufferSize(1000));
        assertInvalid(EngineConfig.builder().ringBufferSize(1));
        assertInvalid(EngineConfig.builder().matcherShards(0));
        assertInvalid(EngineConfig.builder().fragmentLimit(0));
        assertInvalid(EngineConfig.builder().orderPoolSize(0));
        assertInvalid(EngineConfig.builder().snapshotIntervalMs(-1));
        assertInvalid(EngineConfig.builder().snapshotDepth(-1));
        assertInvalid(EngineConfig.builder().publishRetryTimeoutMs(-1));
        assertInvalid(EngineConfig.builder().shutdownTimeoutMs(-1));
        assertInvalid(EngineConfig.builder().termLength(1000));
        assertInvalid(EngineConfig.builder().httpPort(-2));
        assertInvalid(EngineConfig.builder().httpPort(65536));
        assertThrows(NullPointerException.class,
            () -> EngineConfig.builder().addInstrument(EURUSD).httpHost(null).build());
        assertInvalid(EngineConfig.builder().termLength(32768));
        assertInvalid(EngineConfig.builder().termLength(100_000));
        assertNotNull(EngineConfig.builder().termLength(65536).instruments(List.of(EURUSD)).build());
        assertNotNull(EngineConfig.builder().channels(EngineChannels.ipc()).aeronDirectory("d")
            .embeddedMediaDriver(false).driverThreadingMode(ThreadingMode.DEDICATED)
            .deleteAeronDirectoryOnStart(false).addInstrument(EURUSD).build());
        assertThrows(NullPointerException.class,
            () -> EngineConfig.builder().addInstrument(EURUSD).transportMode(null).build());
        assertThrows(NullPointerException.class,
            () -> EngineConfig.builder().addInstrument(EURUSD).waitStrategy(null).build());
        assertThrows(NullPointerException.class,
            () -> EngineConfig.builder().addInstrument(EURUSD).ingressIdleStrategy(null).build());
        assertThrows(NullPointerException.class,
            () -> EngineConfig.builder().addInstrument(EURUSD).driverThreadingMode(null).build());
    }

    private static void assertInvalid(final EngineConfig.Builder builder)
    {
        builder.addInstrument(EURUSD);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @ParameterizedTest
    @EnumSource(WaitStrategyType.class)
    void waitStrategiesCreate(final WaitStrategyType type)
    {
        assertNotNull(type.create());
    }

    @ParameterizedTest
    @EnumSource(IdleStrategyType.class)
    void idleStrategiesCreate(final IdleStrategyType type)
    {
        assertNotNull(type.create());
    }
}
