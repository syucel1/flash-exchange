package com.flashexchange.engine.config;

import com.flashexchange.engine.matching.Instrument;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurabilityHaConfigTest
{
    static final String P = EngineConfig.PREFIX;

    @Test
    void durabilityDefaultsAndProperties()
    {
        final DurabilityConfig d = DurabilityConfig.disabled();
        assertFalse(d.journalEnabled());
        final Properties p = new Properties();
        p.setProperty(P + "journal.enabled", "true");
        p.setProperty(P + "data.dir", "/var/flash");
        p.setProperty(P + "archive.host", "core-a");
        p.setProperty(P + "archive.controlPort", "8010");
        p.setProperty(P + "archive.fileSyncLevel", "1");
        p.setProperty(P + "journal.termLength", "1048576");
        p.setProperty(P + "snapshot.state.intervalMs", "5000");
        p.setProperty(P + "snapshot.state.intervalCommands", "77");
        p.setProperty(P + "snapshot.state.retained", "5");
        p.setProperty(P + "dedup.window", "0");
        final DurabilityConfig c = DurabilityConfig.fromProperties(p, P);
        assertEquals(new DurabilityConfig(true, "/var/flash", "core-a", 8010, 1, 1 << 20, 5000, 77, 5, 0), c);
        assertEquals(c, c.withDedupWindow(0));
        assertEquals(1 << 16, c.withJournalTermLength(1 << 16).journalTermLength());
        assertEquals("h", c.withArchiveControl("h", 1).archiveHost());
    }

    @Test
    void durabilityValidation()
    {
        assertThrows(IllegalArgumentException.class, () -> DurabilityConfig.journal(" "));
        assertThrows(IllegalArgumentException.class, () -> DurabilityConfig.disabled().withArchiveControl("h", 70_000));
        assertThrows(IllegalArgumentException.class, () -> DurabilityConfig.disabled().withArchiveControl("h", -1));
        assertThrows(IllegalArgumentException.class,
            () -> new DurabilityConfig(false, "d", "h", 0, 3, 1 << 16, 0, 0, 1, 0));
        assertThrows(IllegalArgumentException.class,
            () -> new DurabilityConfig(false, "d", "h", 0, -1, 1 << 16, 0, 0, 1, 0));
        assertThrows(IllegalArgumentException.class,
            () -> DurabilityConfig.disabled().withJournalTermLength(1000));
        assertThrows(IllegalArgumentException.class,
            () -> DurabilityConfig.disabled().withJournalTermLength(1 << 15));
        assertThrows(IllegalArgumentException.class, () -> DurabilityConfig.disabled().withSnapshotInterval(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> DurabilityConfig.disabled().withSnapshotInterval(0, -1));
        assertThrows(IllegalArgumentException.class, () -> DurabilityConfig.disabled().withDedupWindow(-1));
        assertThrows(IllegalArgumentException.class,
            () -> new DurabilityConfig(false, "d", "h", 0, 0, 1 << 16, 0, 0, 0, 0));
    }

    @Test
    void haDefaultsPropertiesAndValidation()
    {
        final Properties p = new Properties();
        p.setProperty(P + "ha.enabled", "true");
        p.setProperty(P + "ha.nodeId", "2");
        p.setProperty(P + "ha.mode", "async");
        p.setProperty(P + "ha.self.host", "core-b");
        p.setProperty(P + "ha.self.replicationPort", "1");
        p.setProperty(P + "ha.self.ackPort", "2");
        p.setProperty(P + "ha.self.controlPort", "3");
        p.setProperty(P + "ha.peer.host", "core-a");
        p.setProperty(P + "ha.peer.archivePort", "4");
        p.setProperty(P + "ha.peer.ackPort", "5");
        p.setProperty(P + "ha.arbiter.host", "arb");
        p.setProperty(P + "ha.arbiter.port", "6");
        p.setProperty(P + "ha.heartbeatIntervalMs", "7");
        p.setProperty(P + "ha.ackTimeoutMs", "8");
        final HaConfig ha = HaConfig.fromProperties(p, P);
        assertEquals(new HaConfig(true, 2, ReleaseMode.ASYNC, "core-b", 1, 2, 3, "core-a", 4, 5, "arb", 6, 7, 8), ha);
        assertEquals("aeron:udp?endpoint=h:1", HaConfig.udp("h", 1));
        assertThrows(IllegalArgumentException.class, () -> ha.withSelf(0, "h", 1, 2, 3));
        assertThrows(IllegalArgumentException.class, () -> ha.withTiming(0, 1));
        assertThrows(IllegalArgumentException.class, () -> ha.withTiming(1, 0));
        assertThrows(IllegalArgumentException.class, () -> ha.withArbiter("h", 1, null));
        assertEquals("p", ha.withPeer("p", 1, 2).peerHost());
    }

    @Test
    void engineConfigRequiresJournalAndArchivePortForHa()
    {
        final EngineConfig.Builder b = EngineConfig.builder()
            .addInstrument(new Instrument(1, "EURUSD", 1, 1, 1, 100, 0))
            .ha(HaConfig.disabled().withArbiter("a", 1, ReleaseMode.SYNC));
        assertThrows(IllegalArgumentException.class, b::build);
        b.durability(DurabilityConfig.journal("d"));
        assertThrows(IllegalArgumentException.class, b::build);
        b.durability(DurabilityConfig.journal("d").withArchiveControl("h", 8010));
        final EngineConfig config = b.build();
        assertTrue(config.ha().enabled());
        assertTrue(config.toString().contains("ha="));

        final Properties p = new Properties();
        p.setProperty(P + "instruments", "1,EURUSD,1,1,1,100,0");
        p.setProperty(P + "journal.enabled", "true");
        p.setProperty(P + "data.dir", "x");
        assertTrue(EngineConfig.fromProperties(p).durability().journalEnabled());
    }
}
