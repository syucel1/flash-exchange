package com.flashexchange.engine;

import com.flashexchange.engine.config.EngineConfig;
import com.flashexchange.engine.durability.EngineArchive;
import com.flashexchange.engine.matching.Instrument;
import com.flashexchange.engine.node.EngineConductor;
import com.flashexchange.engine.node.EngineCore;
import com.flashexchange.engine.ops.EngineHttpServer;
import com.flashexchange.engine.pipeline.EnginePipeline;
import com.flashexchange.engine.pipeline.OfferingMessagePublisher;
import com.flashexchange.engine.transport.AeronTransport;
import com.flashexchange.engine.transport.CommandIngress;
import com.flashexchange.protocol.NodeRole;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.SystemNanoClock;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Composition root of the matching engine process:
 * <pre>
 *   Aeron 1001 ──▶ conductor ──▶ Disruptor ring ──▶ matcher shard(s) ‖ journal (Archive 100) ──▶ release gate
 *                                                                                              ──▶ Aeron 2001 / 3001
 * </pre>
 * {@link #launch(EngineConfig)} wires and starts everything. Without HA the engine recovers from its local snapshot
 * and journal (when enabled) before {@code launch} returns; with HA it waits for the arbiter to make it primary or
 * standby. {@link #close()} stops the conductor, writes a final snapshot, drains the pipeline, then closes the
 * archive and Aeron.
 */
public final class MatchingEngineServer implements AutoCloseable
{
    private static final System.Logger LOGGER = System.getLogger(MatchingEngineServer.class.getName());

    private final EngineConfig config;
    private final AeronTransport transport;
    private final EngineArchive archive;
    private final OfferingMessagePublisher executionPublisher;
    private final OfferingMessagePublisher marketDataPublisher;
    private final EngineConductor conductor;
    private final AgentRunner conductorRunner;
    private final AtomicLong ingressErrors = new AtomicLong();
    private final EngineHttpServer httpServer;
    private final long startTimeMs = System.currentTimeMillis();
    private volatile Runnable fencedListener = () -> { };
    private volatile boolean closed;

    private MatchingEngineServer(final EngineConfig config, final AeronTransport transport, final EngineArchive archive)
    {
        this.config = config;
        this.transport = transport;
        this.archive = archive;

        final long retryTimeoutNs = TimeUnit.MILLISECONDS.toNanos(config.publishRetryTimeoutMs());
        this.executionPublisher = new OfferingMessagePublisher(
            "execution", transport.executionPublication()::offer, new BackoffIdleStrategy(),
            SystemNanoClock.INSTANCE, retryTimeoutNs);
        this.marketDataPublisher = new OfferingMessagePublisher(
            "marketData", transport.marketDataPublication()::offer, new BackoffIdleStrategy(),
            SystemNanoClock.INSTANCE, retryTimeoutNs);

        this.conductor = new EngineConductor(config, transport.aeron(), transport.commandSubscription(),
            executionPublisher, marketDataPublisher, archive, this::onFenced);
        if (!config.ha().enabled())
        {
            conductor.startStandalone();
        }
        this.conductorRunner = new AgentRunner(
            config.ingressIdleStrategy().create(), this::onIngressError, null, conductor);
        AgentRunner.startOnThread(conductorRunner);

        EngineHttpServer http = null;
        try
        {
            if (config.httpPort() >= 0)
            {
                http = new EngineHttpServer(config.httpHost(), config.httpPort(), this::isHealthy, this::status);
            }
        }
        catch (final RuntimeException ex)
        {
            CloseHelper.quietClose(conductorRunner);
            final EngineCore core = conductor.core();
            CloseHelper.quietClose(core);
            throw ex;
        }
        this.httpServer = http;
    }

    private void onFenced()
    {
        fencedListener.run();
    }

    /**
     * Register a callback run (on the conductor thread) when the arbiter fences this node. The process must then
     * exit and restart to rejoin as a standby.
     *
     * @param listener callback.
     */
    public void onFenced(final Runnable listener)
    {
        this.fencedListener = listener;
        if (conductor.isFenced())
        {
            listener.run();
        }
    }

    /**
     * Liveness: not closed or fenced, conductor running and Aeron client open. A pending HA node (waiting for
     * the arbiter) is healthy.
     *
     * @return true when the engine can process commands.
     */
    public boolean isHealthy()
    {
        return !closed && !conductor.isFenced() && !conductorRunner.isClosed() && !transport.aeron().isClosed();
    }

    /**
     * Operational status document (configuration and counters), as served on {@code /status}.
     *
     * @return status map.
     */
    public Map<String, Object> status()
    {
        final Map<String, Object> status = new LinkedHashMap<>();
        status.put("status", isHealthy() ? "UP" : "DOWN");
        status.put("version", version());
        status.put("uptimeMs", System.currentTimeMillis() - startTimeMs);
        status.put("transport", config.transportMode().name());
        status.put("aeronDirectory", aeronDirectoryName());
        status.put("commandChannel", config.channels().commandChannel());
        status.put("executionChannel", config.channels().executionChannel());
        status.put("marketDataChannel", config.channels().marketDataChannel());
        status.put("matcherShards", config.matcherShards());
        status.put("instruments", config.instruments().stream().map(Instrument::symbol).toList());
        status.put("journalEnabled", config.durability().journalEnabled());
        status.put("haEnabled", config.ha().enabled());
        status.put("nodeId", config.ha().nodeId());
        status.putAll(conductor.status());
        final EngineCore core = conductor.core();
        if (core != null)
        {
            status.put("commandsReceived", core.ingress().commandsReceived());
            status.put("malformedMessages", core.ingress().malformedMessages());
            status.put("snapshotsScheduled", core.ingress().snapshotsScheduled());
            status.put("lastPublishedSequence", core.pipeline().lastPublishedSequence());
            status.put("eventExceptions", core.pipeline().eventExceptions());
        }
        status.put("ingressErrors", ingressErrors.get());
        status.put("publishers", List.of(publisherStatus(executionPublisher), publisherStatus(marketDataPublisher)));
        return status;
    }

    private static Map<String, Object> publisherStatus(final OfferingMessagePublisher publisher)
    {
        final Map<String, Object> status = new LinkedHashMap<>();
        status.put("name", publisher.name());
        status.put("published", publisher.published());
        status.put("droppedNotConnected", publisher.droppedNotConnected());
        status.put("droppedBackPressured", publisher.droppedBackPressured());
        status.put("failed", publisher.failed());
        return status;
    }

    /**
     * Implementation version from the jar manifest, or {@code dev} when running from classes.
     *
     * @return version string.
     */
    public static String version()
    {
        final String version = MatchingEngineServer.class.getPackage().getImplementationVersion();
        return version == null ? "dev" : version;
    }

    /**
     * Port of the operational HTTP endpoint.
     *
     * @return bound port, or -1 when disabled.
     */
    public int httpPort()
    {
        return httpServer == null ? -1 : httpServer.port();
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
        EngineArchive archive = null;
        try
        {
            if (config.durability().journalEnabled())
            {
                archive = EngineArchive.launch(config.durability(), config.ha().nodeId(),
                    transport.aeron().context().aeronDirectoryName(), transport.aeron());
            }
            final MatchingEngineServer server = new MatchingEngineServer(config, transport, archive);
            LOGGER.log(System.Logger.Level.INFO, "matching engine started: " + config);
            return server;
        }
        catch (final RuntimeException ex)
        {
            CloseHelper.quietCloseAll(archive, transport);
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
        final EngineCore core = conductor.core();
        return core == null ? null : core.ingress();
    }

    /**
     * Processing pipeline.
     *
     * @return pipeline, or {@code null} while an HA node waits for its role.
     */
    public EnginePipeline pipeline()
    {
        final EngineCore core = conductor.core();
        return core == null ? null : core.pipeline();
    }

    /**
     * Node conductor (role, epoch, replication state).
     *
     * @return conductor.
     */
    public EngineConductor conductor()
    {
        return conductor;
    }

    /**
     * Current node role.
     *
     * @return role.
     */
    public NodeRole role()
    {
        return conductor.role();
    }

    /**
     * Local archive, or {@code null} when the journal is disabled.
     *
     * @return archive.
     */
    public EngineArchive archive()
    {
        return archive;
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
     * Errors raised by the conductor agent.
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
        CloseHelper.closeAll(httpServer, conductorRunner);
        try
        {
            conductor.onShutdown();
        }
        catch (final RuntimeException ex)
        {
            LOGGER.log(System.Logger.Level.WARNING, "final snapshot not scheduled", ex);
        }
        CloseHelper.closeAll(conductor.core(), archive, transport);
        LOGGER.log(System.Logger.Level.INFO, "matching engine stopped: " + executionPublisher + " " +
            marketDataPublisher);
    }

    /**
     * Stop abruptly, as if the process had been killed: no final snapshot, in-flight commands are abandoned and
     * the archive is closed with whatever it has recorded. Used by failover and recovery tests.
     */
    public void halt()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        CloseHelper.quietCloseAll(httpServer, conductorRunner);
        final EngineCore core = conductor.core();
        if (core != null)
        {
            core.halt();
        }
        CloseHelper.quietCloseAll(archive, transport);
        LOGGER.log(System.Logger.Level.INFO, "matching engine halted");
    }
}
