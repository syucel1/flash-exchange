package com.flashexchange.bench;

import com.flashexchange.engine.config.WaitStrategyType;
import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.engine.pipeline.EnginePipeline;
import com.flashexchange.engine.pipeline.OrderCommand;
import com.flashexchange.protocol.OrderType;
import com.flashexchange.protocol.Side;
import com.flashexchange.protocol.TimeInForce;
import com.lmax.disruptor.RingBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Throughput of the Disruptor pipeline (ingress publication, matcher shards, publisher) with a no-op transport.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED", "-Xms2g", "-Xmx2g"})
@State(Scope.Benchmark)
public class PipelineBenchmark
{
    private static final int BATCH = 1_000;

    /** Number of matcher shards. */
    @Param({"1", "2"})
    public int shards;

    private EnginePipeline pipeline;
    private RingBuffer<OrderCommand> ring;
    private long published = -1;

    /**
     * Start the pipeline.
     */
    @Setup(Level.Trial)
    public void setUp()
    {
        pipeline = new EnginePipeline(
            List.of(new Instrument(1, "EURUSD", 1, 1, 1, 1_000_000_000L, 0),
                new Instrument(2, "GBPUSD", 1, 1, 1, 1_000_000_000L, 0)),
            shards, 1 << 16, WaitStrategyType.BUSY_SPIN.create(), 1 << 20, 0,
            (buffer, offset, length) -> true, (buffer, offset, length) -> true, 5_000);
        ring = pipeline.start();
    }

    /**
     * Stop the pipeline.
     */
    @TearDown(Level.Trial)
    public void tearDown()
    {
        pipeline.close();
    }

    /**
     * Publish a batch of alternating crossing orders on two instruments and wait until all are published.
     *
     * @return last sequence.
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public long publishBatch()
    {
        for (int i = 0; i < BATCH; i++)
        {
            final long seq = ring.next();
            final Side side = (seq & 1) == 0 ? Side.BUY : Side.SELL;
            ring.get(seq).newOrder(seq, 1 + (seq & 31), 1 + (int)((seq >> 1) & 1), side.code(),
                OrderType.LIMIT.code(), TimeInForce.GTC.code(), (byte)0, false, 100_000, 100).stamp(seq, seq);
            ring.publish(seq);
            published = seq;
        }
        while (pipeline.lastPublishedSequence() < published)
        {
            Thread.onSpinWait();
        }
        return published;
    }
}
