package com.flashexchange.engine.ha;

import com.flashexchange.engine.ops.EngineHttpServer;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.util.Map;

/**
 * Arbiter process: optional embedded media driver, Aeron client, the {@link Arbiter} agent on its own thread and an
 * optional HTTP {@code /health} and {@code /status} endpoint.
 */
public final class ArbiterServer implements AutoCloseable
{
    private final MediaDriver driver;
    private final Aeron aeron;
    private final Arbiter arbiter;
    private final AgentRunner runner;
    private final EngineHttpServer http;

    private ArbiterServer(final ArbiterConfig config)
    {
        MediaDriver d = null;
        Aeron a = null;
        AgentRunner r = null;
        try
        {
            String directory = config.aeronDirectory();
            if (config.embeddedMediaDriver())
            {
                final MediaDriver.Context context = new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true);
                if (directory != null)
                {
                    context.aeronDirectoryName(directory);
                }
                d = MediaDriver.launch(context);
                directory = d.aeronDirectoryName();
            }
            final Aeron.Context clientContext = new Aeron.Context();
            if (directory != null)
            {
                clientContext.aeronDirectoryName(directory);
            }
            a = Aeron.connect(clientContext);
            final Arbiter agent = new Arbiter(config, a, System::nanoTime);
            r = new AgentRunner(new SleepingMillisIdleStrategy(1), Throwable::printStackTrace, null, agent);
            AgentRunner.startOnThread(r);
            this.driver = d;
            this.aeron = a;
            this.arbiter = agent;
            this.runner = r;
            this.http = config.httpPort() >= 0 ?
                new EngineHttpServer(config.httpHost(), config.httpPort(), this::isHealthy, this::status) : null;
        }
        catch (final RuntimeException ex)
        {
            CloseHelper.quietCloseAll(r, a, d);
            throw ex;
        }
    }

    /**
     * Start an arbiter.
     *
     * @param config configuration.
     * @return running arbiter.
     */
    public static ArbiterServer launch(final ArbiterConfig config)
    {
        return new ArbiterServer(config);
    }

    /**
     * Liveness.
     *
     * @return true while the agent runs.
     */
    public boolean isHealthy()
    {
        return !runner.isClosed() && !aeron.isClosed();
    }

    /**
     * Status document.
     *
     * @return status.
     */
    public Map<String, Object> status()
    {
        final Map<String, Object> status = arbiter.status();
        status.put("status", isHealthy() ? "UP" : "DOWN");
        return status;
    }

    /**
     * The arbiter agent.
     *
     * @return arbiter.
     */
    public Arbiter arbiter()
    {
        return arbiter;
    }

    /**
     * HTTP port.
     *
     * @return port or -1.
     */
    public int httpPort()
    {
        return http == null ? -1 : http.port();
    }

    @Override
    public void close()
    {
        CloseHelper.closeAll(http, runner, aeron, driver);
    }
}
