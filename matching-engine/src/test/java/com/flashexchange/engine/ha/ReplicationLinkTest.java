package com.flashexchange.engine.ha;

import com.flashexchange.engine.config.ReleaseMode;
import com.flashexchange.protocol.ReplicationAckFlyweight;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ReplicationLinkTest
{
    @TempDir
    Path tempDir;

    MediaDriver driver;
    Aeron aeron;
    Subscription ackSubscription;
    Publication ackPublication;
    final AtomicLong clock = new AtomicLong();

    @BeforeEach
    void setUp()
    {
        driver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(tempDir.toString())
            .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        ackSubscription = aeron.addSubscription("aeron:ipc", 102);
        ackPublication = aeron.addPublication("aeron:ipc", 102);
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietCloseAll(ackPublication, ackSubscription, aeron, driver);
    }

    ReplicationLink primary(final ReleaseMode mode)
    {
        final ReplicationLink link = new ReplicationLink(mode, 50, 1, ackSubscription, null, clock::get);
        link.leaseUntil(Long.MAX_VALUE / 2);
        return link;
    }

    void awaitAcked(final ReplicationLink link, final long sequence)
    {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (link.ackedSequence() < sequence)
        {
            assertTrue(System.nanoTime() < deadline, "ack received");
            link.pollAcks(sequence);
        }
    }

    @Test
    void syncWaitsForTheStandbyAck() throws Exception
    {
        final ReplicationLink link = primary(ReleaseMode.SYNC);
        final ReplicationLink standby = new ReplicationLink(ReleaseMode.SYNC, 50, 2, null, ackPublication, clock::get);
        standby.epoch(4);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!ackSubscription.isConnected())
        {
            assertTrue(System.nanoTime() < deadline);
            Thread.yield();
        }
        assertFalse(link.standbyAttached(), "no ack yet");
        assertTrue(link.awaitAck(0), "no standby attached: released immediately");

        standby.sendKeepAlive(0);
        awaitAcked(link, 0);
        assertTrue(link.standbyAttached());

        final CompletableFuture<Boolean> released = CompletableFuture.supplyAsync(() -> link.awaitAck(5));
        Thread.sleep(20);
        assertFalse(released.isDone(), "waiting for ack 5");
        standby.sendAck(5);
        while (!released.isDone())
        {
            link.pollAcks(5);
        }
        assertTrue(released.get());
        assertEquals(5, link.ackedSequence());
        assertEquals(ReleaseMode.SYNC, link.mode());
        assertTrue(link.acksReceived() >= 2);
    }

    @Test
    void ackTimeoutDegradesUntilTheStandbyCatchesUp() throws Exception
    {
        final ReplicationLink link = primary(ReleaseMode.SYNC);
        final ReplicationLink standby = new ReplicationLink(ReleaseMode.SYNC, 50, 2, null, ackPublication, clock::get);
        while (!ackSubscription.isConnected())
        {
            Thread.yield();
        }
        standby.sendKeepAlive(0);
        awaitAcked(link, 0);

        final CompletableFuture<Boolean> released = CompletableFuture.supplyAsync(() -> link.awaitAck(9));
        Thread.sleep(20);
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(60));
        assertTrue(released.get(5, TimeUnit.SECONDS), "released after the ack timeout");
        assertTrue(link.isDegraded());
        assertEquals(1, link.degradations());
        assertTrue(link.awaitAck(10), "degraded: no waiting");

        standby.sendAck(10);
        awaitAcked(link, 10);
        link.pollAcks(10);
        assertFalse(link.isDegraded(), "caught up");
    }

    @Test
    void withholdsWithoutLeaseAndDropsWhenFenced() throws Exception
    {
        final ReplicationLink link = new ReplicationLink(ReleaseMode.ASYNC, 50, 1, ackSubscription, null, clock::get);
        assertFalse(link.leaseValid(), "no lease granted yet");
        final CompletableFuture<Boolean> released = CompletableFuture.supplyAsync(() -> link.awaitAck(1));
        Thread.sleep(20);
        assertFalse(released.isDone(), "withheld without a lease");
        link.leaseUntil(clock.get() + 1_000);
        assertTrue(released.get(5, TimeUnit.SECONDS));

        clock.addAndGet(2_000);
        final CompletableFuture<Boolean> fenced = CompletableFuture.supplyAsync(() -> link.awaitAck(2));
        Thread.sleep(20);
        link.fence();
        assertFalse(fenced.get(5, TimeUnit.SECONDS), "dropped once fenced");
        assertTrue(link.isFenced());

        link.leaseRequired(false);
        assertTrue(link.leaseValid());
        link.onAckImageChange(null);
        link.sendAck(1);
        link.sendKeepAlive(1);
    }

    @Test
    void closingUnblocksAWaitingPublisher() throws Exception
    {
        final ReplicationLink link = new ReplicationLink(ReleaseMode.ASYNC, 50, 1, null, null, clock::get);
        final CompletableFuture<Boolean> waiting = CompletableFuture.supplyAsync(() -> link.awaitAck(1));
        Thread.sleep(20);
        link.close();
        final Exception ex = assertThrows(Exception.class, () -> waiting.get(5, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof IllegalStateException);
        assertEquals(0, link.pollAcks(1));
        assertFalse(link.standbyAttached());
        assertEquals(ReplicationAckFlyweight.TEMPLATE_ID, new ReplicationAckFlyweight().templateId());
    }
}
