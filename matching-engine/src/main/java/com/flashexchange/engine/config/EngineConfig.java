package com.flashexchange.engine.config;

import com.flashexchange.engine.matching.Instrument;
import io.aeron.driver.ThreadingMode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Matching engine configuration. Built with {@link #builder()} or loaded from properties (keys prefixed
 * {@code flash.engine.}; see {@code matching-engine.properties} for a documented example).
 */
public final class EngineConfig
{
    /** Prefix of every configuration property. */
    public static final String PREFIX = "flash.engine.";

    private final TransportMode transportMode;
    private final EngineChannels channels;
    private final String aeronDirectory;
    private final boolean embeddedMediaDriver;
    private final ThreadingMode driverThreadingMode;
    private final boolean deleteAeronDirectoryOnStart;
    private final int termLength;
    private final List<Instrument> instruments;
    private final int ringBufferSize;
    private final WaitStrategyType waitStrategy;
    private final IdleStrategyType ingressIdleStrategy;
    private final int matcherShards;
    private final int fragmentLimit;
    private final int orderPoolSize;
    private final long snapshotIntervalMs;
    private final int snapshotDepth;
    private final long publishRetryTimeoutMs;
    private final long shutdownTimeoutMs;
    private final String httpHost;
    private final int httpPort;

    private EngineConfig(final Builder b)
    {
        this.transportMode = Objects.requireNonNull(b.transportMode, "transportMode");
        this.channels = b.channels != null ? b.channels : defaultChannels(b);
        this.aeronDirectory = b.aeronDirectory;
        this.embeddedMediaDriver = b.embeddedMediaDriver;
        this.driverThreadingMode = Objects.requireNonNull(b.driverThreadingMode, "driverThreadingMode");
        this.deleteAeronDirectoryOnStart = b.deleteAeronDirectoryOnStart;
        this.termLength = b.termLength;
        this.instruments = List.copyOf(b.instruments);
        this.ringBufferSize = b.ringBufferSize;
        this.waitStrategy = Objects.requireNonNull(b.waitStrategy, "waitStrategy");
        this.ingressIdleStrategy = Objects.requireNonNull(b.ingressIdleStrategy, "ingressIdleStrategy");
        this.matcherShards = b.matcherShards;
        this.fragmentLimit = b.fragmentLimit;
        this.orderPoolSize = b.orderPoolSize;
        this.snapshotIntervalMs = b.snapshotIntervalMs;
        this.snapshotDepth = b.snapshotDepth;
        this.publishRetryTimeoutMs = b.publishRetryTimeoutMs;
        this.shutdownTimeoutMs = b.shutdownTimeoutMs;
        this.httpHost = Objects.requireNonNull(b.httpHost, "httpHost");
        this.httpPort = b.httpPort;
        validate();
    }

    private static EngineChannels defaultChannels(final Builder b)
    {
        return b.transportMode == TransportMode.IPC ?
            EngineChannels.ipc() :
            EngineChannels.udpEngineSide(
                b.udpHost, b.udpCommandPort, b.udpExecutionControlPort, b.udpMarketDataControlPort);
    }

    private void validate()
    {
        if (instruments.isEmpty())
        {
            throw new IllegalArgumentException("at least one instrument must be configured");
        }
        if (ringBufferSize < 2 || Integer.bitCount(ringBufferSize) != 1)
        {
            throw new IllegalArgumentException("ringBufferSize must be a power of two: " + ringBufferSize);
        }
        if (matcherShards < 1)
        {
            throw new IllegalArgumentException("matcherShards must be >= 1: " + matcherShards);
        }
        if (fragmentLimit < 1)
        {
            throw new IllegalArgumentException("fragmentLimit must be >= 1: " + fragmentLimit);
        }
        if (orderPoolSize < 1)
        {
            throw new IllegalArgumentException("orderPoolSize must be >= 1: " + orderPoolSize);
        }
        if (snapshotIntervalMs < 0 || snapshotDepth < 0 || publishRetryTimeoutMs < 0 || shutdownTimeoutMs < 0)
        {
            throw new IllegalArgumentException("intervals, depths and timeouts must be >= 0");
        }
        if (httpPort < -1 || httpPort > 65535)
        {
            throw new IllegalArgumentException(
                "httpPort must be -1 (disabled), 0 (ephemeral) or a port: " + httpPort);
        }
        if (termLength != 0 && (termLength < 65536 || Integer.bitCount(termLength) != 1))
        {
            throw new IllegalArgumentException("termLength must be 0 (driver default) or a power of two >= 64KiB");
        }
    }

    /**
     * New builder with defaults.
     *
     * @return builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Load from an optional properties file, overlaid with any {@code flash.engine.*} system properties.
     *
     * @param file properties file or {@code null}.
     * @return configuration.
     * @throws IOException if the file cannot be read.
     */
    public static EngineConfig load(final Path file) throws IOException
    {
        final Properties properties = new Properties();
        if (file != null)
        {
            try (InputStream in = Files.newInputStream(file))
            {
                properties.load(in);
            }
        }
        for (final Map.Entry<Object, Object> e : System.getProperties().entrySet())
        {
            if (e.getKey().toString().startsWith(PREFIX))
            {
                properties.put(e.getKey(), e.getValue());
            }
        }
        return fromProperties(properties);
    }

    /**
     * Build from properties. Unspecified keys take the builder defaults.
     *
     * @param p properties.
     * @return configuration.
     */
    public static EngineConfig fromProperties(final Properties p)
    {
        final Builder b = builder();
        b.transportMode(TransportMode.valueOf(get(p, "transport", b.transportMode.name()).toUpperCase(Locale.ROOT)));
        b.udpHost(get(p, "udp.host", b.udpHost));
        b.udpCommandPort(Integer.parseInt(get(p, "udp.commandPort", String.valueOf(b.udpCommandPort))));
        b.udpExecutionControlPort(Integer.parseInt(
            get(p, "udp.executionControlPort", String.valueOf(b.udpExecutionControlPort))));
        b.udpMarketDataControlPort(Integer.parseInt(
            get(p, "udp.marketDataControlPort", String.valueOf(b.udpMarketDataControlPort))));

        final String commandChannel = p.getProperty(PREFIX + "channel.command");
        final String executionChannel = p.getProperty(PREFIX + "channel.execution");
        final String marketDataChannel = p.getProperty(PREFIX + "channel.marketData");
        final int commandStream = Integer.parseInt(
            get(p, "stream.command", String.valueOf(EngineChannels.COMMAND_STREAM_ID)));
        final int executionStream = Integer.parseInt(
            get(p, "stream.execution", String.valueOf(EngineChannels.EXECUTION_STREAM_ID)));
        final int marketDataStream = Integer.parseInt(
            get(p, "stream.marketData", String.valueOf(EngineChannels.MARKET_DATA_STREAM_ID)));
        final EngineChannels defaults = defaultChannels(b);
        b.channels(new EngineChannels(
            commandChannel != null ? commandChannel : defaults.commandChannel(), commandStream,
            executionChannel != null ? executionChannel : defaults.executionChannel(), executionStream,
            marketDataChannel != null ? marketDataChannel : defaults.marketDataChannel(), marketDataStream));

        b.aeronDirectory(p.getProperty(PREFIX + "aeron.dir"));
        b.embeddedMediaDriver(Boolean.parseBoolean(
            get(p, "aeron.embeddedMediaDriver", String.valueOf(b.embeddedMediaDriver))));
        b.driverThreadingMode(ThreadingMode.valueOf(
            get(p, "aeron.driverThreadingMode", b.driverThreadingMode.name()).toUpperCase(Locale.ROOT)));
        b.deleteAeronDirectoryOnStart(Boolean.parseBoolean(
            get(p, "aeron.deleteDirOnStart", String.valueOf(b.deleteAeronDirectoryOnStart))));
        b.termLength(Integer.parseInt(get(p, "aeron.termLength", String.valueOf(b.termLength))));

        b.ringBufferSize(Integer.parseInt(get(p, "ringBufferSize", String.valueOf(b.ringBufferSize))));
        b.waitStrategy(WaitStrategyType.valueOf(
            get(p, "waitStrategy", b.waitStrategy.name()).toUpperCase(Locale.ROOT)));
        b.ingressIdleStrategy(IdleStrategyType.valueOf(
            get(p, "ingressIdleStrategy", b.ingressIdleStrategy.name()).toUpperCase(Locale.ROOT)));
        b.matcherShards(Integer.parseInt(get(p, "matcherShards", String.valueOf(b.matcherShards))));
        b.fragmentLimit(Integer.parseInt(get(p, "fragmentLimit", String.valueOf(b.fragmentLimit))));
        b.orderPoolSize(Integer.parseInt(get(p, "orderPoolSize", String.valueOf(b.orderPoolSize))));
        b.snapshotIntervalMs(Long.parseLong(get(p, "snapshot.intervalMs", String.valueOf(b.snapshotIntervalMs))));
        b.snapshotDepth(Integer.parseInt(get(p, "snapshot.depth", String.valueOf(b.snapshotDepth))));
        b.publishRetryTimeoutMs(Long.parseLong(
            get(p, "publish.retryTimeoutMs", String.valueOf(b.publishRetryTimeoutMs))));
        b.shutdownTimeoutMs(Long.parseLong(get(p, "shutdownTimeoutMs", String.valueOf(b.shutdownTimeoutMs))));
        b.httpHost(get(p, "http.host", b.httpHost));
        b.httpPort(Integer.parseInt(get(p, "http.port", String.valueOf(b.httpPort))));

        final String instruments = p.getProperty(PREFIX + "instruments");
        if (instruments != null)
        {
            b.instruments(parseInstruments(instruments));
        }
        return b.build();
    }

    private static String get(final Properties p, final String key, final String defaultValue)
    {
        final String value = p.getProperty(PREFIX + key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    /**
     * Parse instruments from {@code id,symbol,tickSize,lotSize,minQty,maxQty,collarBps} entries separated by
     * {@code ;}.
     *
     * @param spec instrument specification.
     * @return instruments.
     */
    public static List<Instrument> parseInstruments(final String spec)
    {
        final List<Instrument> result = new ArrayList<>();
        for (final String entry : spec.split(";"))
        {
            if (entry.isBlank())
            {
                continue;
            }
            final String[] f = entry.trim().split(",");
            if (f.length != 7)
            {
                throw new IllegalArgumentException(
                    "instrument must be id,symbol,tickSize,lotSize,minQty,maxQty,collarBps: " + entry);
            }
            result.add(new Instrument(
                Integer.parseInt(f[0].trim()),
                f[1].trim(),
                Long.parseLong(f[2].trim()),
                Long.parseLong(f[3].trim()),
                Long.parseLong(f[4].trim()),
                Long.parseLong(f[5].trim()),
                Integer.parseInt(f[6].trim())));
        }
        return result;
    }

    /**
     * Transport mode.
     *
     * @return mode.
     */
    public TransportMode transportMode()
    {
        return transportMode;
    }

    /**
     * Engine-side channels and streams.
     *
     * @return channels.
     */
    public EngineChannels channels()
    {
        return channels;
    }

    /**
     * Aeron directory, or {@code null} for the Aeron default.
     *
     * @return directory.
     */
    public String aeronDirectory()
    {
        return aeronDirectory;
    }

    /**
     * Whether to launch an embedded media driver (otherwise connect to an external one).
     *
     * @return true for embedded.
     */
    public boolean embeddedMediaDriver()
    {
        return embeddedMediaDriver;
    }

    /**
     * Embedded media driver threading mode.
     *
     * @return threading mode.
     */
    public ThreadingMode driverThreadingMode()
    {
        return driverThreadingMode;
    }

    /**
     * Whether the embedded driver deletes a stale Aeron directory on start.
     *
     * @return true to delete.
     */
    public boolean deleteAeronDirectoryOnStart()
    {
        return deleteAeronDirectoryOnStart;
    }

    /**
     * Term buffer length for IPC and network publications of the embedded driver (0 = driver default).
     *
     * @return term length.
     */
    public int termLength()
    {
        return termLength;
    }

    /**
     * Instruments traded on this engine.
     *
     * @return instruments.
     */
    public List<Instrument> instruments()
    {
        return instruments;
    }

    /**
     * Ring buffer size.
     *
     * @return size (power of two).
     */
    public int ringBufferSize()
    {
        return ringBufferSize;
    }

    /**
     * Disruptor wait strategy.
     *
     * @return strategy type.
     */
    public WaitStrategyType waitStrategy()
    {
        return waitStrategy;
    }

    /**
     * Idle strategy of the ingress poller.
     *
     * @return strategy type.
     */
    public IdleStrategyType ingressIdleStrategy()
    {
        return ingressIdleStrategy;
    }

    /**
     * Number of matcher threads.
     *
     * @return shard count.
     */
    public int matcherShards()
    {
        return matcherShards;
    }

    /**
     * Maximum fragments read per ingress poll.
     *
     * @return fragment limit.
     */
    public int fragmentLimit()
    {
        return fragmentLimit;
    }

    /**
     * Pre-allocated orders per shard.
     *
     * @return pool size.
     */
    public int orderPoolSize()
    {
        return orderPoolSize;
    }

    /**
     * Interval of periodic book snapshots (0 = disabled).
     *
     * @return milliseconds.
     */
    public long snapshotIntervalMs()
    {
        return snapshotIntervalMs;
    }

    /**
     * Levels per side in snapshots (0 = full depth).
     *
     * @return depth.
     */
    public int snapshotDepth()
    {
        return snapshotDepth;
    }

    /**
     * How long to retry a back-pressured publication before dropping the message.
     *
     * @return milliseconds.
     */
    public long publishRetryTimeoutMs()
    {
        return publishRetryTimeoutMs;
    }

    /**
     * How long shutdown waits for in-flight commands.
     *
     * @return milliseconds.
     */
    public long shutdownTimeoutMs()
    {
        return shutdownTimeoutMs;
    }

    /**
     * Bind address of the operational HTTP endpoint.
     *
     * @return host.
     */
    public String httpHost()
    {
        return httpHost;
    }

    /**
     * Port of the operational HTTP endpoint (-1 = disabled, 0 = ephemeral).
     *
     * @return port.
     */
    public int httpPort()
    {
        return httpPort;
    }

    @Override
    public String toString()
    {
        return "EngineConfig{transport=" + transportMode + ", channels=" + channels + ", aeronDirectory=" +
            aeronDirectory + ", embeddedMediaDriver=" + embeddedMediaDriver + ", driverThreadingMode=" +
            driverThreadingMode + ", instruments=" + instruments.size() + ", ringBufferSize=" + ringBufferSize +
            ", waitStrategy=" + waitStrategy + ", matcherShards=" + matcherShards + ", httpPort=" + httpPort + "}";
    }

    /**
     * Builder with development-friendly defaults (IPC, embedded driver, sleeping wait strategy).
     */
    public static final class Builder
    {
        private TransportMode transportMode = TransportMode.IPC;
        private EngineChannels channels;
        private String udpHost = "localhost";
        private int udpCommandPort = EngineChannels.COMMAND_PORT;
        private int udpExecutionControlPort = EngineChannels.EXECUTION_CONTROL_PORT;
        private int udpMarketDataControlPort = EngineChannels.MARKET_DATA_CONTROL_PORT;
        private String aeronDirectory;
        private boolean embeddedMediaDriver = true;
        private ThreadingMode driverThreadingMode = ThreadingMode.SHARED;
        private boolean deleteAeronDirectoryOnStart = true;
        private int termLength;
        private List<Instrument> instruments = new ArrayList<>();
        private int ringBufferSize = 65536;
        private WaitStrategyType waitStrategy = WaitStrategyType.SLEEPING;
        private IdleStrategyType ingressIdleStrategy = IdleStrategyType.BACKOFF;
        private int matcherShards = 1;
        private int fragmentLimit = 256;
        private int orderPoolSize = 65536;
        private long snapshotIntervalMs = 1000;
        private int snapshotDepth;
        private long publishRetryTimeoutMs = 1000;
        private long shutdownTimeoutMs = 5000;
        private String httpHost = "0.0.0.0";
        private int httpPort = -1;

        private Builder()
        {
        }

        /**
         * Transport mode used to derive default channels.
         *
         * @param value mode.
         * @return this.
         */
        public Builder transportMode(final TransportMode value)
        {
            transportMode = value;
            return this;
        }

        /**
         * Explicit channels (overrides the transport mode defaults).
         *
         * @param value channels.
         * @return this.
         */
        public Builder channels(final EngineChannels value)
        {
            channels = value;
            return this;
        }

        /**
         * Engine host for UDP mode.
         *
         * @param value host or IP.
         * @return this.
         */
        public Builder udpHost(final String value)
        {
            udpHost = value;
            return this;
        }

        /**
         * UDP command port.
         *
         * @param value port.
         * @return this.
         */
        public Builder udpCommandPort(final int value)
        {
            udpCommandPort = value;
            return this;
        }

        /**
         * UDP execution control port.
         *
         * @param value port.
         * @return this.
         */
        public Builder udpExecutionControlPort(final int value)
        {
            udpExecutionControlPort = value;
            return this;
        }

        /**
         * UDP market data control port.
         *
         * @param value port.
         * @return this.
         */
        public Builder udpMarketDataControlPort(final int value)
        {
            udpMarketDataControlPort = value;
            return this;
        }

        /**
         * Aeron directory.
         *
         * @param value directory or {@code null} for the default.
         * @return this.
         */
        public Builder aeronDirectory(final String value)
        {
            aeronDirectory = value;
            return this;
        }

        /**
         * Whether to embed a media driver.
         *
         * @param value true to embed.
         * @return this.
         */
        public Builder embeddedMediaDriver(final boolean value)
        {
            embeddedMediaDriver = value;
            return this;
        }

        /**
         * Embedded driver threading mode.
         *
         * @param value mode.
         * @return this.
         */
        public Builder driverThreadingMode(final ThreadingMode value)
        {
            driverThreadingMode = value;
            return this;
        }

        /**
         * Delete a stale Aeron directory on start.
         *
         * @param value true to delete.
         * @return this.
         */
        public Builder deleteAeronDirectoryOnStart(final boolean value)
        {
            deleteAeronDirectoryOnStart = value;
            return this;
        }

        /**
         * Term length of the embedded driver (0 = default).
         *
         * @param value term length.
         * @return this.
         */
        public Builder termLength(final int value)
        {
            termLength = value;
            return this;
        }

        /**
         * Instruments.
         *
         * @param value instruments.
         * @return this.
         */
        public Builder instruments(final List<Instrument> value)
        {
            instruments = new ArrayList<>(value);
            return this;
        }

        /**
         * Add one instrument.
         *
         * @param value instrument.
         * @return this.
         */
        public Builder addInstrument(final Instrument value)
        {
            instruments.add(value);
            return this;
        }

        /**
         * Ring buffer size.
         *
         * @param value power of two.
         * @return this.
         */
        public Builder ringBufferSize(final int value)
        {
            ringBufferSize = value;
            return this;
        }

        /**
         * Disruptor wait strategy.
         *
         * @param value strategy.
         * @return this.
         */
        public Builder waitStrategy(final WaitStrategyType value)
        {
            waitStrategy = value;
            return this;
        }

        /**
         * Ingress idle strategy.
         *
         * @param value strategy.
         * @return this.
         */
        public Builder ingressIdleStrategy(final IdleStrategyType value)
        {
            ingressIdleStrategy = value;
            return this;
        }

        /**
         * Matcher shard count.
         *
         * @param value shards.
         * @return this.
         */
        public Builder matcherShards(final int value)
        {
            matcherShards = value;
            return this;
        }

        /**
         * Ingress fragment limit.
         *
         * @param value limit.
         * @return this.
         */
        public Builder fragmentLimit(final int value)
        {
            fragmentLimit = value;
            return this;
        }

        /**
         * Order pool size per shard.
         *
         * @param value size.
         * @return this.
         */
        public Builder orderPoolSize(final int value)
        {
            orderPoolSize = value;
            return this;
        }

        /**
         * Snapshot interval.
         *
         * @param value milliseconds (0 = disabled).
         * @return this.
         */
        public Builder snapshotIntervalMs(final long value)
        {
            snapshotIntervalMs = value;
            return this;
        }

        /**
         * Snapshot depth.
         *
         * @param value levels per side (0 = full).
         * @return this.
         */
        public Builder snapshotDepth(final int value)
        {
            snapshotDepth = value;
            return this;
        }

        /**
         * Publication retry timeout.
         *
         * @param value milliseconds.
         * @return this.
         */
        public Builder publishRetryTimeoutMs(final long value)
        {
            publishRetryTimeoutMs = value;
            return this;
        }

        /**
         * Shutdown drain timeout.
         *
         * @param value milliseconds.
         * @return this.
         */
        public Builder shutdownTimeoutMs(final long value)
        {
            shutdownTimeoutMs = value;
            return this;
        }

        /**
         * Bind address of the operational HTTP endpoint.
         *
         * @param value host.
         * @return this.
         */
        public Builder httpHost(final String value)
        {
            httpHost = value;
            return this;
        }

        /**
         * Port of the operational HTTP endpoint (-1 = disabled, 0 = ephemeral).
         *
         * @param value port.
         * @return this.
         */
        public Builder httpPort(final int value)
        {
            httpPort = value;
            return this;
        }

        /**
         * Build and validate.
         *
         * @return configuration.
         */
        public EngineConfig build()
        {
            return new EngineConfig(this);
        }
    }
}
