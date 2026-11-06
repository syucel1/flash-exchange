package com.flashexchange.engine.tools;

import com.flashexchange.engine.config.EngineChannels;
import com.flashexchange.engine.config.TransportMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemoTraderConfigTest
{
    @Test
    void defaults()
    {
        final DemoTraderConfig c = DemoTraderConfig.fromProperties(new Properties());
        assertEquals(TransportMode.IPC, c.transport());
        assertFalse(c.embeddedMediaDriver());
        assertNull(c.aeronDirectory());
        assertEquals(EngineChannels.ipc(), c.channels());
        assertEquals(1, c.instruments().size());
        assertEquals(1000, c.userBase());
        assertEquals(20, c.users());
        assertEquals(50, c.ordersPerSecond());
        assertEquals(0, c.maxOrders());
        assertEquals(0, c.durationSeconds());
        assertEquals(10, c.reportIntervalSeconds());
        assertEquals(20, c.spreadTicks());
        assertEquals(10, c.marketOrderPercent());
        assertEquals(15, c.cancelPercent());
        assertEquals(10, c.maxLots());
        assertEquals(42, c.seed());
    }

    @Test
    void udpFromFileAndSystemProperties(@TempDir final Path dir) throws Exception
    {
        final Path file = dir.resolve("demo.properties");
        Files.writeString(file, String.join("\n",
            "flash.demo.transport=udp",
            "flash.demo.udp.engineHost=engine",
            "flash.demo.udp.clientHost=client",
            "flash.demo.udp.commandPort=1",
            "flash.demo.udp.executionControlPort=2",
            "flash.demo.udp.marketDataControlPort=3",
            "flash.demo.instruments=1,EURUSD,108345,1,100;2,GBPUSD,127184,1,100;",
            "flash.demo.aeron.dir=/tmp/x"));
        System.setProperty("flash.demo.users", "7");
        try
        {
            final DemoTraderConfig c = DemoTraderConfig.load(file);
            assertEquals(TransportMode.UDP, c.transport());
            assertTrue(c.embeddedMediaDriver());
            assertEquals("/tmp/x", c.aeronDirectory());
            assertEquals(7, c.users());
            assertEquals(EngineChannels.udpClientSide("engine", "client", 1, 2, 3), c.channels());
            assertEquals(List.of(new DemoTraderConfig.DemoInstrument(1, "EURUSD", 108345, 1, 100),
                new DemoTraderConfig.DemoInstrument(2, "GBPUSD", 127184, 1, 100)), c.instruments());
            assertEquals(1, DemoTraderConfig.load(null).instruments().size());
        }
        finally
        {
            System.clearProperty("flash.demo.users");
        }
    }

    @Test
    void validation()
    {
        assertThrows(IllegalArgumentException.class, () -> DemoTraderConfig.parseInstruments("1,A,1,1"));
        assertThrows(IllegalArgumentException.class, () -> with("flash.demo.instruments", " ; "));
        assertThrows(IllegalArgumentException.class, () -> with("flash.demo.users", "0"));
        assertThrows(IllegalArgumentException.class, () -> with("flash.demo.ordersPerSecond", "0"));
        assertThrows(IllegalArgumentException.class, () -> with("flash.demo.spreadTicks", "0"));
        assertThrows(IllegalArgumentException.class, () -> with("flash.demo.maxLots", "0"));
        assertThrows(IllegalArgumentException.class, () -> with("flash.demo.marketOrderPercent", "-1"));
        assertThrows(IllegalArgumentException.class, () -> with("flash.demo.cancelPercent", "-1"));
        assertThrows(IllegalArgumentException.class, () -> with("flash.demo.cancelPercent", "95"));
    }

    private static DemoTraderConfig with(final String key, final String value)
    {
        final Properties p = new Properties();
        p.setProperty(key, value);
        return DemoTraderConfig.fromProperties(p);
    }
}
