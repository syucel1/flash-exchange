package com.flashexchange.engine.transport;

import com.flashexchange.engine.config.EngineChannels;
import com.flashexchange.engine.config.EngineConfig;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;

/**
 * Aeron resources of the engine: an optional embedded media driver, the Aeron client, the command subscription and
 * the two exclusive publications (execution events and market data). Whether traffic flows over shared memory or
 * reliable UDP is decided purely by the configured channels.
 */
public final class AeronTransport implements AutoCloseable
{
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Subscription commandSubscription;
    private final ExclusivePublication executionPublication;
    private final ExclusivePublication marketDataPublication;

    private AeronTransport(
        final MediaDriver mediaDriver,
        final Aeron aeron,
        final Subscription commandSubscription,
        final ExclusivePublication executionPublication,
        final ExclusivePublication marketDataPublication)
    {
        this.mediaDriver = mediaDriver;
        this.aeron = aeron;
        this.commandSubscription = commandSubscription;
        this.executionPublication = executionPublication;
        this.marketDataPublication = marketDataPublication;
    }

    /**
     * Launch (optionally) a media driver, connect and create the engine's subscription and publications.
     *
     * @param config engine configuration.
     * @return the connected transport.
     */
    public static AeronTransport connect(final EngineConfig config)
    {
        MediaDriver driver = null;
        Aeron aeron = null;
        try
        {
            String aeronDirectory = config.aeronDirectory();
            if (config.embeddedMediaDriver())
            {
                final MediaDriver.Context driverContext = new MediaDriver.Context()
                    .threadingMode(config.driverThreadingMode())
                    .dirDeleteOnStart(config.deleteAeronDirectoryOnStart())
                    .dirDeleteOnShutdown(true);
                if (aeronDirectory != null)
                {
                    driverContext.aeronDirectoryName(aeronDirectory);
                }
                if (config.termLength() > 0)
                {
                    driverContext.ipcTermBufferLength(config.termLength())
                        .publicationTermBufferLength(config.termLength());
                }
                driver = MediaDriver.launch(driverContext);
                aeronDirectory = driver.aeronDirectoryName();
            }

            final Aeron.Context clientContext = new Aeron.Context();
            if (aeronDirectory != null)
            {
                clientContext.aeronDirectoryName(aeronDirectory);
            }
            aeron = Aeron.connect(clientContext);

            final EngineChannels channels = config.channels();
            final Subscription commands =
                aeron.addSubscription(channels.commandChannel(), channels.commandStreamId());
            final ExclusivePublication executions =
                aeron.addExclusivePublication(channels.executionChannel(), channels.executionStreamId());
            final ExclusivePublication marketData =
                aeron.addExclusivePublication(channels.marketDataChannel(), channels.marketDataStreamId());

            return new AeronTransport(driver, aeron, commands, executions, marketData);
        }
        catch (final RuntimeException ex)
        {
            CloseHelper.quietCloseAll(aeron, driver);
            throw ex;
        }
    }

    /**
     * Embedded media driver.
     *
     * @return the driver or {@code null} when using an external one.
     */
    public MediaDriver mediaDriver()
    {
        return mediaDriver;
    }

    /**
     * Aeron client.
     *
     * @return the client.
     */
    public Aeron aeron()
    {
        return aeron;
    }

    /**
     * Subscription receiving commands.
     *
     * @return command subscription.
     */
    public Subscription commandSubscription()
    {
        return commandSubscription;
    }

    /**
     * Publication for execution events.
     *
     * @return execution publication.
     */
    public ExclusivePublication executionPublication()
    {
        return executionPublication;
    }

    /**
     * Publication for market data.
     *
     * @return market data publication.
     */
    public ExclusivePublication marketDataPublication()
    {
        return marketDataPublication;
    }

    /**
     * Close publications, subscription, client and (if embedded) the driver.
     */
    @Override
    public void close()
    {
        CloseHelper.closeAll(
            commandSubscription, executionPublication, marketDataPublication, aeron, mediaDriver);
    }
}
