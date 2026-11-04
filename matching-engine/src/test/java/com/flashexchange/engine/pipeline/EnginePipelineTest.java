package com.flashexchange.engine.pipeline;

import com.flashexchange.engine.config.WaitStrategyType;
import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.protocol.EngineEventListener;
import com.flashexchange.protocol.ExecutionReportFlyweight;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import com.lmax.disruptor.RingBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnginePipelineTest
{
    static final List<Instrument> INSTRUMENTS = List.of(
        new Instrument(1, "A", 1, 1, 1, 1_000_000, 0),
        new Instrument(2, "B", 1, 1, 1, 1_000_000, 0),
        new Instrument(3, "C", 1, 1, 1, 1_000_000, 0));

    static void publishOrder(
        final RingBuffer<OrderCommand> ring, final int instrument, final Side side, final long price, final long qty)
    {
        final long seq = ring.next();
        ring.get(seq).newOrder(seq, 1, instrument, side.code(), OrderType.LIMIT.code(), TimeInForce.GTC.code(),
            (byte)0, false, price, qty).stamp(seq, seq);
        ring.publish(seq);
    }

    static void awaitPublished(final EnginePipeline pipeline, final long sequence)
    {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
        {
            while (pipeline.lastPublishedSequence() < sequence)
            {
                Thread.onSpinWait();
            }
        });
    }

    @ParameterizedTest
    @EnumSource(WaitStrategyType.class)
    void shardedPipelinePublishesInEngineSequenceOrder(final WaitStrategyType waitStrategy)
    {
        final CapturingPublisher exec = new CapturingPublisher();
        final CapturingPublisher md = new CapturingPublisher();
        try (EnginePipeline pipeline = new EnginePipeline(
            INSTRUMENTS, 2, 1024, waitStrategy.create(), 64, 0, exec, md, 5_000))
        {
            assertNull(pipeline.ringBuffer());
            final RingBuffer<OrderCommand> ring = pipeline.start();
            assertSame(ring, pipeline.ringBuffer());
            assertEquals(2, pipeline.shardCount());

            final int orders = 3_000;
            for (int i = 0; i < orders; i++)
            {
                final int instrument = 1 + (i % 3);
                publishOrder(ring, instrument, i % 2 == 0 ? Side.BUY : Side.SELL, 100, 1);
            }
            awaitPublished(pipeline, orders - 1);

            final List<Long> sequences = new ArrayList<>();
            exec.replay(new EngineEventListener()
            {
                @Override
                public void onExecutionReport(final ExecutionReportFlyweight report)
                {
                    sequences.add(report.engineSequence());
                }
            });
            for (int i = 1; i < sequences.size(); i++)
            {
                assertTrue(sequences.get(i) >= sequences.get(i - 1), "execution output ordered by engine sequence");
            }
            assertTrue(sequences.size() >= orders);
            assertEquals(0, pipeline.eventExceptions());
            assertNull(pipeline.engine(0).book(1), "instrument 1 lives on shard 1");
            assertTrue(pipeline.engine(0).book(2) != null);
        }
    }

    @Test
    void failingCommandIsCountedAndPipelineContinues()
    {
        final CapturingPublisher md = new CapturingPublisher();
        final int[] calls = new int[1];
        try (EnginePipeline pipeline = new EnginePipeline(INSTRUMENTS, 1, 64,
            WaitStrategyType.YIELDING.create(), 8, 0, (buffer, offset, length) ->
            {
                if (calls[0]++ == 0)
                {
                    throw new IllegalStateException("transport failure");
                }
                return true;
            }, md, 1_000))
        {
            final RingBuffer<OrderCommand> ring = pipeline.start();
            publishOrder(ring, 1, Side.BUY, 100, 1);
            publishOrder(ring, 1, Side.BUY, 100, 1);
            awaitPublished(pipeline, 1);
            assertEquals(1, pipeline.eventExceptions());
            assertEquals(2, pipeline.engine(0).book(1).orderCount());
        }
    }

    @Test
    void closeIsIdempotentAndSafeBeforeStart() throws Exception
    {
        final EnginePipeline notStarted = new EnginePipeline(INSTRUMENTS, 1, 64,
            WaitStrategyType.BLOCKING.create(), 8, 0, (b, o, l) -> true, (b, o, l) -> true, 100);
        notStarted.close();
        notStarted.close();

        final CountDownLatch entered = new CountDownLatch(1);
        final EnginePipeline stuck = new EnginePipeline(INSTRUMENTS, 1, 64,
            WaitStrategyType.SLEEPING.create(), 8, 0, (b, o, l) ->
            {
                entered.countDown();
                try
                {
                    Thread.sleep(2_000);
                }
                catch (final InterruptedException ignore)
                {
                    Thread.currentThread().interrupt();
                }
                return true;
            }, (b, o, l) -> true, 50);
        final RingBuffer<OrderCommand> ring = stuck.start();
        publishOrder(ring, 1, Side.BUY, 100, 1);
        publishOrder(ring, 1, Side.BUY, 100, 1);
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        final long start = System.nanoTime();
        stuck.close();
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2), "times out and halts");
    }

    @Test
    void rejectsInvalidShardCount()
    {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () ->
            new EnginePipeline(INSTRUMENTS, 0, 64, WaitStrategyType.SLEEPING.create(), 8, 0,
                (b, o, l) -> true, (b, o, l) -> true, 100));
    }
}
