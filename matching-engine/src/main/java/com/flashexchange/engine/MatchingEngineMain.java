package com.flashexchange.engine;

import com.flashexchange.engine.config.EngineConfig;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.nio.file.Path;

/**
 * Process entry point.
 * <pre>
 *   java -jar matching-engine.jar [config.properties]
 * </pre>
 * Any {@code flash.engine.*} system property overrides the file. The process runs until SIGINT/SIGTERM.
 */
public final class MatchingEngineMain
{
    private MatchingEngineMain()
    {
    }

    /** Exit status of a node fenced by the arbiter; the supervisor restarts it and it rejoins as standby. */
    public static final int EXIT_FENCED = 3;

    /**
     * Run the engine until the barrier is signalled or the node is fenced.
     *
     * @param config  configuration.
     * @param barrier signalled on shutdown.
     * @return true if the engine stopped because it was fenced.
     */
    public static boolean run(final EngineConfig config, final ShutdownSignalBarrier barrier)
    {
        final MatchingEngineServer server = MatchingEngineServer.launch(config);
        server.onFenced(barrier::signal);
        try
        {
            barrier.await();
        }
        finally
        {
            server.close();
        }
        return server.conductor().isFenced();
    }

    /**
     * Main.
     *
     * @param args optional path of a properties file.
     * @throws Exception on configuration errors.
     */
    public static void main(final String[] args) throws Exception
    {
        final EngineConfig config = EngineConfig.load(args.length > 0 ? Path.of(args[0]) : null);
        if (run(config, new ShutdownSignalBarrier()))
        {
            System.exit(EXIT_FENCED);
        }
    }
}
