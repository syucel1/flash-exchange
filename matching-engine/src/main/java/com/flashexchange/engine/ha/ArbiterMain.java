package com.flashexchange.engine.ha;

import org.agrona.concurrent.ShutdownSignalBarrier;

import java.nio.file.Path;

/**
 * Command line entry point of the arbiter: {@code java -cp matching-engine-all.jar
 * com.flashexchange.engine.ha.ArbiterMain [arbiter.properties]}.
 */
public final class ArbiterMain
{
    private ArbiterMain()
    {
    }

    /**
     * Run until SIGINT/SIGTERM.
     *
     * @param args optional properties file.
     * @throws Exception if the configuration cannot be read.
     */
    public static void main(final String[] args) throws Exception
    {
        final ArbiterConfig config = ArbiterConfig.load(args.length > 0 ? Path.of(args[0]) : null);
        try (ArbiterServer server = ArbiterServer.launch(config))
        {
            System.out.println("arbiter started: " + config);
            new ShutdownSignalBarrier().await();
            server.status();
        }
    }
}
