package com.flashexchange.engine.pipeline;

import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.engine.matching.MatchingEngine;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The engine's internal event-processing pipeline, built on the LMAX Disruptor:
 * <pre>
 *   ingress (single producer) ──▶ [ ring of OrderCommand ] ──▶ matcher shard 0 ┐
 *                                                          ──▶ matcher shard 1 ┼──▶ publisher ──▶ Aeron 2001 / 3001
 *                                                          ──▶ matcher shard N ┘
 * </pre>
 * Matcher shards run in parallel on their own threads, each owning a disjoint set of instruments. The publisher
 * runs strictly after all matchers for a given slot and handles slots in sequence order, so output order is
 * deterministic regardless of the number of shards.
 */
public final class EnginePipeline implements AutoCloseable
{
    private final Disruptor<OrderCommand> disruptor;
    private final MatchingEngine[] engines;
    private final PublisherEventHandler publisherHandler;
    private final CountingExceptionHandler exceptionHandler = new CountingExceptionHandler();
    private final long shutdownTimeoutMs;
    private RingBuffer<OrderCommand> ringBuffer;
    private boolean closed;

    /**
     * Build (but do not start) a pipeline.
     *
     * @param instruments         all instruments; each is assigned to the shard
     *                            {@link MatcherEventHandler#shardOf(int, int)}.
     * @param shardCount          number of matcher threads.
     * @param ringBufferSize      ring size (power of two).
     * @param waitStrategy        Disruptor wait strategy for the consumers.
     * @param orderPoolSize       pre-allocated orders per shard.
     * @param snapshotDepth       levels per side in snapshots (0 = full depth).
     * @param executionPublisher  destination of private events.
     * @param marketDataPublisher destination of market data.
     * @param shutdownTimeoutMs   how long {@link #close()} waits for in-flight commands to drain.
     */
    public EnginePipeline(
        final Collection<Instrument> instruments,
        final int shardCount,
        final int ringBufferSize,
        final WaitStrategy waitStrategy,
        final int orderPoolSize,
        final int snapshotDepth,
        final MessagePublisher executionPublisher,
        final MessagePublisher marketDataPublisher,
        final long shutdownTimeoutMs)
    {
        if (shardCount < 1)
        {
            throw new IllegalArgumentException("shardCount must be >= 1: " + shardCount);
        }
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.disruptor = new Disruptor<>(
            OrderCommand::new, ringBufferSize, new NamedThreadFactory(), ProducerType.SINGLE, waitStrategy);
        this.disruptor.setDefaultExceptionHandler(exceptionHandler);

        this.engines = new MatchingEngine[shardCount];
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final EventHandler<OrderCommand>[] matchers = new EventHandler[shardCount];
        for (int shard = 0; shard < shardCount; shard++)
        {
            final List<Instrument> shardInstruments = new ArrayList<>();
            for (final Instrument instrument : instruments)
            {
                if (MatcherEventHandler.shardOf(instrument.id(), shardCount) == shard)
                {
                    shardInstruments.add(instrument);
                }
            }
            final EncodingEventSink sink = new EncodingEventSink();
            engines[shard] = new MatchingEngine(shardInstruments, sink, orderPoolSize, snapshotDepth);
            matchers[shard] = new MatcherEventHandler(shard, shardCount, engines[shard], sink);
        }

        this.publisherHandler = new PublisherEventHandler(executionPublisher, marketDataPublisher);
        disruptor.handleEventsWith(matchers).then(publisherHandler);
    }

    /**
     * Start the consumer threads.
     *
     * @return the ring buffer the ingress publishes to.
     */
    public RingBuffer<OrderCommand> start()
    {
        ringBuffer = disruptor.start();
        return ringBuffer;
    }

    /**
     * Ring buffer (after {@link #start()}).
     *
     * @return ring buffer.
     */
    public RingBuffer<OrderCommand> ringBuffer()
    {
        return ringBuffer;
    }

    /**
     * Matching engine of a shard. Only safe to inspect while the pipeline is quiescent.
     *
     * @param shard index.
     * @return the engine.
     */
    public MatchingEngine engine(final int shard)
    {
        return engines[shard];
    }

    /**
     * Number of matcher shards.
     *
     * @return shard count.
     */
    public int shardCount()
    {
        return engines.length;
    }

    /**
     * Sequence of the last slot fully published.
     *
     * @return sequence, -1 before the first.
     */
    public long lastPublishedSequence()
    {
        return publisherHandler.lastPublishedSequence();
    }

    /**
     * Commands whose processing threw an exception.
     *
     * @return count.
     */
    public long eventExceptions()
    {
        return exceptionHandler.eventExceptions();
    }

    /**
     * Drain in-flight commands (up to the shutdown timeout) and stop the consumer threads. Idempotent.
     */
    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        if (ringBuffer == null)
        {
            return;
        }
        try
        {
            disruptor.shutdown(shutdownTimeoutMs, TimeUnit.MILLISECONDS);
        }
        catch (final TimeoutException ex)
        {
            disruptor.halt();
        }
    }

    private static final class NamedThreadFactory implements ThreadFactory
    {
        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(final Runnable runnable)
        {
            final Thread thread = new Thread(runnable, "engine-pipeline-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
