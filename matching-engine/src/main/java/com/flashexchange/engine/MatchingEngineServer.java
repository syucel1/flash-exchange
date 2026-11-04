package com.flashexchange.engine;

import com.flashexchange.engine.config.EngineConfig;
import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.engine.pipeline.EnginePipeline;
import com.flashexchange.engine.pipeline.OfferingMessagePublisher;
import com.flashexchange.engine.pipeline.OrderCommand;
import com.flashexchange.engine.transport.AeronTransport;
import com.flashexchange.engine.transport.CommandIngress;
import com.lmax.disruptor.RingBuffer;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.agrona.concurrent.SystemNanoClock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Composition root of the matching engine process:
 * <pre>
 *   Aeron 1001 ──▶ CommandIngress ──▶ Disruptor ring ──▶ matcher shard(s) ──▶ publisher ──▶ Aeron 2001 / 3001
 * </pre>
 * {@link #launch(EngineConfig)} wires and starts everything; {@link #close()} stops the ingress, drains the
 * pipeline, then closes Aeron (and the embedded driver).
 */
public final class MatchingEngineServer implements AutoCloseable
{
    private static final System.Logger LOGGER = System.getLogger(MatchingEngineServer.class.getName());

    private final EngineConfig config;
    private final AeronTransport transport;
    private final OfferingMessagePublisher executionPublisher;
    private final OfferingMessagePublisher marketDataPublisher;
    private final EnginePipeline pipeline;
    private final CommandIngress ingress;
    private final AgentRunner ingressRunner;
    private final AtomicLong ingressErrors = new AtomicLong();
    private volatile boolean closed;

    private MatchingEngineServer(final EngineConfig config, final AeronTransport transport)
    {
        this.config = config;
        this.transport = transport;

        final long retryTimeoutNs = TimeUnit.MILLISECONDS.toNanos(config.publishRetryTimeoutMs());
        this.executionPublisher = new OfferingMessagePublisher(
            "execution", transport.executionPublication()::offer, new BackoffIdleStrategy(),
            SystemNanoClock.INSTANCE, retryTimeoutNs);
        this.marketDataPublisher = new OfferingMessagePublisher(
            "marketData", transport.marketDataPublication()::offer, new BackoffIdleStrategy(),
            SystemNanoClock.INSTANCE, retryTimeoutNs);

        this.pipeline = new EnginePipeline(
            config.instruments(),
            config.matcherShards(),
            config.ringBufferSize(),
            config.waitStrategy().create(),
            config.orderPoolSize(),
            config.snapshotDepth(),
            executionPublisher,
            marketDataPublisher,
            config.shutdownTimeoutMs());
        final RingBuffer<OrderCommand> ringBuffer = pipeline.start();

        this.ingress = new CommandIngress(
            transport.commandSubscription(),
            ringBuffer,
            new SystemEpochNanoClock(),
            config.fragmentLimit(),
            config.instruments().stream().mapToInt(Instrument::id).toArray(),
            TimeUnit.MILLISECONDS.toNanos(config.snapshotIntervalMs()));
        this.ingressRunner = new AgentRunner(
            config.ingressIdleStrategy().create(), this::onIngressError, null, ingress);
        AgentRunner.startOnThread(ingressRunner);
    }

    /**
     * Start an engine.
     *
     * @param config configuration.
     * @return the running engine.
     */
    public static MatchingEngineServer launch(final EngineConfig config)
    {
        final AeronTransport transport = AeronTransport.connect(config);
        try
        {
            final MatchingEngineServer server = new MatchingEngineServer(config, transport);
            LOGGER.log(System.Logger.Level.INFO, "matching engine started: " + config);
            return server;
        }
        catch (final RuntimeException ex)
        {
            transport.close();
            throw ex;
        }
    }

    void onIngressError(final Throwable throwable)
    {
        ingressErrors.incrementAndGet();
        LOGGER.log(System.Logger.Level.ERROR, "ingress error", throwable);
    }

    /**
     * Configuration in use.
     *
     * @return configuration.
     */
    public EngineConfig config()
    {
        return config;
    }

    /**
     * Aeron resources.
     *
     * @return transport.
     */
    public AeronTransport transport()
    {
        return transport;
    }

    /**
     * Aeron directory used by the engine (clients on the same box connect to it).
     *
     * @return directory name.
     */
    public String aeronDirectoryName()
    {
        return transport.aeron().context().aeronDirectoryName();
    }

    /**
     * Ingress agent (counters).
     *
     * @return ingress.
     */
    public CommandIngress ingress()
    {
        return ingress;
    }

    /**
     * Processing pipeline.
     *
     * @return pipeline.
     */
    public EnginePipeline pipeline()
    {
        return pipeline;
    }

    /**
     * Execution event publisher (counters).
     *
     * @return publisher.
     */
    public OfferingMessagePublisher executionPublisher()
    {
        return executionPublisher;
    }

    /**
     * Market data publisher (counters).
     *
     * @return publisher.
     */
    public OfferingMessagePublisher marketDataPublisher()
    {
        return marketDataPublisher;
    }

    /**
     * Errors raised by the ingress agent.
     *
     * @return count.
     */
    public long ingressErrors()
    {
        return ingressErrors.get();
    }

    /**
     * Stop accepting commands, drain the pipeline and release Aeron resources. Idempotent.
     */
    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        CloseHelper.closeAll(ingressRunner, pipeline, transport);
        LOGGER.log(System.Logger.Level.INFO, "matching engine stopped: " + executionPublisher + " " +
            marketDataPublisher);
    }
}
