package com.flashexchange.engine.ha;

import com.flashexchange.engine.config.HaConfig;
import com.flashexchange.protocol.ArbiterCommandFlyweight;
import com.flashexchange.protocol.ArbiterCommandType;
import com.flashexchange.protocol.NodeHeartbeatFlyweight;
import com.flashexchange.protocol.NodeRole;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Arbiter decisions driven by scripted node heartbeats and a manual clock.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ArbiterTest
{
    @TempDir
    Path tempDir;

    MediaDriver driver;
    Aeron aeron;
    Arbiter arbiter;
    final AtomicLong clock = new AtomicLong(1_000_000_000L);
    int base;
    Publication heartbeats;
    final FakeNode[] nodes = new FakeNode[3];

    final class FakeNode
    {
        final int id;
        final Subscription commands;
        final List<long[]> received = new ArrayList<>();
        final ArbiterCommandFlyweight decoder = new ArbiterCommandFlyweight();

        FakeNode(final int id)
        {
            this.id = id;
            this.commands = aeron.addSubscription(HaConfig.udp("localhost", base + 10 + id),
                HaConfig.ARBITER_COMMAND_STREAM_ID);
        }

        void heartbeat(final NodeRole role, final long epoch, final long last, final long released,
            final boolean caughtUp, final boolean peerVisible)
        {
            final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(NodeHeartbeatFlyweight.ENCODED_LENGTH));
            new NodeHeartbeatFlyweight().wrapForEncode(buffer, 0).epoch(epoch).lastSequence(last)
                .releasedSequence(released).nodeId(id).role(role).caughtUp((byte)(caughtUp ? 1 : 0))
                .peerVisible((byte)(peerVisible ? 1 : 0));
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (heartbeats.offer(buffer, 0, NodeHeartbeatFlyweight.ENCODED_LENGTH) < 0)
            {
                assertTrue(System.nanoTime() < deadline, "heartbeat sent");
                Thread.yield();
            }
            // let it arrive and be processed
            final long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(30);
            while (System.nanoTime() < until)
            {
                arbiter.doWork();
            }
        }

        List<long[]> drain()
        {
            final long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
            while (System.nanoTime() < until)
            {
                commands.poll((buffer, offset, length, header) ->
                {
                    decoder.wrapForDecode(buffer, offset);
                    received.add(new long[] {decoder.commandCode(), decoder.epoch(), decoder.primaryNodeId(),
                        decoder.leaseDurationMs()});
                }, 16);
            }
            final List<long[]> result = new ArrayList<>(received);
            received.clear();
            return result;
        }

        boolean got(final ArbiterCommandType type)
        {
            return drain().stream().anyMatch(c -> c[0] == type.code());
        }
    }

    @BeforeEach
    void setUp()
    {
        base = 22_000 + ThreadLocalRandom.current().nextInt(30_000);
        driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("aeron").toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        startArbiter();
        heartbeats = aeron.addPublication(HaConfig.udp("localhost", base), HaConfig.HEARTBEAT_STREAM_ID);
        nodes[1] = new FakeNode(1);
        nodes[2] = new FakeNode(2);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!heartbeats.isConnected() || !nodes[1].commands.isConnected() || !nodes[2].commands.isConnected())
        {
            assertTrue(System.nanoTime() < deadline, "connected");
            arbiter.doWork();
            Thread.yield();
        }
    }

    ArbiterConfig config()
    {
        return new ArbiterConfig("localhost", base,
            List.of(new ArbiterConfig.Node(1, "localhost", base + 11),
                new ArbiterConfig.Node(2, "localhost", base + 12)),
            300, 200, 100, 1_000, tempDir.resolve("arbiter.state").toString(), null, false, "localhost", -1);
    }

    void startArbiter()
    {
        arbiter = new Arbiter(config(), aeron, clock::get);
    }

    @AfterEach
    void tearDown()
    {
        if (arbiter != null)
        {
            arbiter.onClose();
        }
        CloseHelper.quietCloseAll(aeron, driver);
    }

    void advance(final long ms)
    {
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
        arbiter.doWork();
    }

    void establish()
    {
        nodes[1].heartbeat(NodeRole.PENDING, 0, -1, -1, false, false);
        assertTrue(nodes[1].got(ArbiterCommandType.PROMOTE), "preferred node promoted");
        assertEquals(1, arbiter.epoch());
        nodes[1].heartbeat(NodeRole.PRIMARY, 1, -1, -1, false, false);
        nodes[2].heartbeat(NodeRole.PENDING, 0, -1, -1, false, false);
        advance(250);
        nodes[1].heartbeat(NodeRole.PRIMARY, 1, 10, 10, false, false);
        nodes[2].heartbeat(NodeRole.PENDING, 0, -1, -1, false, false);
        assertTrue(nodes[2].got(ArbiterCommandType.FOLLOW), "pending node told to follow");
        nodes[2].heartbeat(NodeRole.STANDBY, 1, 10, -1, true, true);
    }

    @Test
    void electsLeasesAndFailsOverOnlyAfterTheLeaseExpired()
    {
        establish();
        advance(150);
        nodes[1].heartbeat(NodeRole.PRIMARY, 1, 20, 20, false, false);
        nodes[2].heartbeat(NodeRole.STANDBY, 1, 20, -1, true, true);
        assertTrue(nodes[1].got(ArbiterCommandType.LEASE), "lease renewed");

        // primary goes silent: no promotion before primary timeout and lease + guard
        advance(150);
        nodes[2].heartbeat(NodeRole.STANDBY, 1, 20, -1, true, false);
        assertFalse(nodes[2].got(ArbiterCommandType.PROMOTE), "too early");
        advance(300);
        nodes[2].heartbeat(NodeRole.STANDBY, 1, 20, -1, true, false);
        assertFalse(nodes[2].got(ArbiterCommandType.PROMOTE), "lease granted while the primary was alive");
        advance(150);
        nodes[2].heartbeat(NodeRole.STANDBY, 1, 20, -1, true, false);
        final List<long[]> commands = nodes[2].drain();
        assertTrue(commands.stream().anyMatch(c -> c[0] == ArbiterCommandType.PROMOTE.code() && c[1] == 2),
            "standby promoted with epoch 2");
        assertEquals(2, arbiter.primaryId());
        assertEquals(2, arbiter.promotions());

        // the old primary reappears claiming the old epoch: fenced
        nodes[2].heartbeat(NodeRole.PRIMARY, 2, 21, 21, false, false);
        nodes[1].heartbeat(NodeRole.PRIMARY, 1, 20, 20, false, false);
        assertTrue(nodes[1].got(ArbiterCommandType.FENCE), "stale primary fenced");

        // after its restart it is told to follow
        advance(250);
        nodes[2].heartbeat(NodeRole.PRIMARY, 2, 21, 21, false, false);
        nodes[1].heartbeat(NodeRole.PENDING, 1, -1, -1, false, false);
        assertTrue(nodes[1].got(ArbiterCommandType.FOLLOW));

        final Map<String, Object> status = arbiter.status();
        assertEquals(2L, status.get("epoch"));
        assertEquals(1L, status.get("failovers"));
    }

    @Test
    void doesNotPromoteAStandbyThatMissesReleasedOutput()
    {
        establish();
        nodes[1].heartbeat(NodeRole.PRIMARY, 1, 50, 50, false, false);
        nodes[2].heartbeat(NodeRole.STANDBY, 1, 40, -1, true, true);
        advance(700);
        nodes[2].heartbeat(NodeRole.STANDBY, 1, 40, -1, true, false);
        assertFalse(nodes[2].got(ArbiterCommandType.PROMOTE), "behind the released sequence");
        assertEquals(1, arbiter.primaryId());
        assertTrue(arbiter.status().get("lastDecision").toString().contains("no eligible standby"));

        // a standby that is not caught up is not promoted either
        nodes[2].heartbeat(NodeRole.STANDBY, 1, 60, -1, false, false);
        assertFalse(nodes[2].got(ArbiterCommandType.PROMOTE));
    }

    @Test
    void restartedPrimaryLeadsAgainUnderANewEpochAndStandbyIsRefollowed()
    {
        establish();
        // the standby lost the replication stream: told to follow again
        advance(250);
        nodes[1].heartbeat(NodeRole.PRIMARY, 1, 30, 30, false, false);
        nodes[2].heartbeat(NodeRole.STANDBY, 1, 30, -1, true, false);
        assertTrue(nodes[2].got(ArbiterCommandType.FOLLOW));

        // the primary restarted quickly (before any failover)
        nodes[1].heartbeat(NodeRole.PENDING, 1, -1, -1, false, false);
        final List<long[]> commands = nodes[1].drain();
        assertTrue(commands.stream().anyMatch(c -> c[0] == ArbiterCommandType.PROMOTE.code() && c[1] == 2));
        // still recovering: the promotion is repeated with the same epoch
        advance(250);
        nodes[1].heartbeat(NodeRole.PENDING, 1, -1, -1, false, false);
        assertTrue(nodes[1].drain().stream().anyMatch(c -> c[0] == ArbiterCommandType.PROMOTE.code() && c[1] == 2));
        assertEquals(2, arbiter.epoch());
    }

    @Test
    void persistsStateAcrossRestartsAndFallsBackToAnotherNodeAfterGrace() throws Exception
    {
        // node 1 never shows up: node 2 is elected after the grace period
        nodes[2].heartbeat(NodeRole.PENDING, 0, -1, -1, false, false);
        assertFalse(nodes[2].got(ArbiterCommandType.PROMOTE), "waits for the preferred node");
        advance(1_100);
        nodes[2].heartbeat(NodeRole.PENDING, 0, -1, -1, false, false);
        assertTrue(nodes[2].got(ArbiterCommandType.PROMOTE));

        final Properties stored = new Properties();
        try (var in = Files.newInputStream(tempDir.resolve("arbiter.state")))
        {
            stored.load(in);
        }
        assertEquals("1", stored.getProperty("epoch"));
        assertEquals("2", stored.getProperty("primary"));

        arbiter.onClose();
        startArbiter();
        assertEquals(1, arbiter.epoch());
        assertEquals(2, arbiter.primaryId());
        assertEquals("arbiter", arbiter.roleName());
    }

    @Test
    void configParsing()
    {
        final Properties p = new Properties();
        p.setProperty("flash.arbiter.nodes", "1=a:1, 2=b:2,");
        p.setProperty("flash.arbiter.leaseDurationMs", "900");
        final ArbiterConfig config = ArbiterConfig.fromProperties(p);
        assertEquals(List.of(new ArbiterConfig.Node(1, "a", 1), new ArbiterConfig.Node(2, "b", 2)), config.nodes());
        assertEquals(900, config.leaseDurationMs());
        assertThrows(IllegalArgumentException.class, () -> ArbiterConfig.parseNodes("1:a"));
        assertThrows(IllegalArgumentException.class, () -> new ArbiterConfig("h", 1,
            ArbiterConfig.parseNodes("1=a:1"), 300, 1, 0, 0, "f", null, false, "h", -1));
        assertThrows(IllegalArgumentException.class, () -> new ArbiterConfig("h", 1, config.nodes(), 1, 1, 0, 0,
            "f", null, false, "h", -1));
    }
}
