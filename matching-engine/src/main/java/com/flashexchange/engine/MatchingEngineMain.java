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

    /**
     * Run the engine until the barrier is signalled.
     *
     * @param config  configuration.
     * @param barrier signalled on shutdown.
     */
    public static void run(final EngineConfig config, final ShutdownSignalBarrier barrier)
    {
        final MatchingEngineServer server = MatchingEngineServer.launch(config);
        try
        {
            barrier.await();
        }
        finally
        {
            server.close();
        }
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
        run(config, new ShutdownSignalBarrier());
    }
}
