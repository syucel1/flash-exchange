package com.flashexchange.engine.tools;

import com.flashexchange.engine.config.EngineChannels;
import com.flashexchange.engine.config.TransportMode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration of the {@link DemoTrader} load generator. Keys are prefixed {@code flash.demo.}; see
 * {@code docker/config/demo-*.properties}.
 *
 * @param transport             IPC (share the engine's media driver) or UDP (own embedded driver).
 * @param aeronDirectory        Aeron directory (IPC: the engine's / external driver's; UDP: the local one).
 * @param embeddedMediaDriver   launch an embedded media driver (normally true for UDP, false for IPC).
 * @param engineHost            engine address (UDP).
 * @param clientHost            local address to receive on (UDP).
 * @param commandPort           engine command port (UDP).
 * @param executionControlPort  engine execution control port (UDP).
 * @param marketDataControlPort engine market data control port (UDP).
 * @param instruments           instruments to trade.
 * @param userBase              first user id used by the simulator.
 * @param users                 number of simulated users.
 * @param ordersPerSecond       target order rate.
 * @param maxOrders             stop after this many orders (0 = unlimited).
 * @param durationSeconds       stop after this many seconds (0 = unlimited).
 * @param reportIntervalSeconds console report period (0 = no periodic report).
 * @param spreadTicks           limit prices are placed within this many ticks around the reference price.
 * @param marketOrderPercent    share of market orders.
 * @param cancelPercent         share of cancels of the simulator's own resting orders.
 * @param maxLots               order size is 1..maxLots lots.
 * @param seed                  random seed.
 */
public record DemoTraderConfig(
    TransportMode transport,
    String aeronDirectory,
    boolean embeddedMediaDriver,
    String engineHost,
    String clientHost,
    int commandPort,
    int executionControlPort,
    int marketDataControlPort,
    List<DemoInstrument> instruments,
    long userBase,
    int users,
    int ordersPerSecond,
    long maxOrders,
    long durationSeconds,
    long reportIntervalSeconds,
    int spreadTicks,
    int marketOrderPercent,
    int cancelPercent,
    int maxLots,
    long seed)
{
    /** Property prefix. */
    public static final String PREFIX = "flash.demo.";

    /**
     * One instrument the simulator trades.
     *
     * @param id        instrument id.
     * @param symbol    display symbol.
     * @param basePrice initial reference price (instrument units).
     * @param tickSize  tick size.
     * @param lotSize   lot size.
     */
    public record DemoInstrument(int id, String symbol, long basePrice, long tickSize, long lotSize)
    {
    }

    /**
     * Validate.
     */
    public DemoTraderConfig
    {
        if (instruments.isEmpty())
        {
            throw new IllegalArgumentException("at least one instrument is required");
        }
        if (users < 1 || ordersPerSecond < 1 || spreadTicks < 1 || maxLots < 1)
        {
            throw new IllegalArgumentException("users, ordersPerSecond, spreadTicks and maxLots must be >= 1");
        }
        if (marketOrderPercent < 0 || cancelPercent < 0 || marketOrderPercent + cancelPercent > 100)
        {
            throw new IllegalArgumentException("marketOrderPercent + cancelPercent must be within 0..100");
        }
        instruments = List.copyOf(instruments);
    }

    /**
     * Client-side channels for the configured transport.
     *
     * @return channels.
     */
    public EngineChannels channels()
    {
        return transport == TransportMode.IPC ?
            EngineChannels.ipc() :
            EngineChannels.udpClientSide(
                engineHost, clientHost, commandPort, executionControlPort, marketDataControlPort);
    }

    /**
     * Load from an optional file overlaid with {@code flash.demo.*} system properties.
     *
     * @param file properties file or {@code null}.
     * @return configuration.
     * @throws IOException if the file cannot be read.
     */
    public static DemoTraderConfig load(final Path file) throws IOException
    {
        final Properties p = new Properties();
        if (file != null)
        {
            try (InputStream in = Files.newInputStream(file))
            {
                p.load(in);
            }
        }
        for (final Map.Entry<Object, Object> e : System.getProperties().entrySet())
        {
            if (e.getKey().toString().startsWith(PREFIX))
            {
                p.put(e.getKey(), e.getValue());
            }
        }
        return fromProperties(p);
    }

    /**
     * Build from properties with defaults for missing keys.
     *
     * @param p properties.
     * @return configuration.
     */
    public static DemoTraderConfig fromProperties(final Properties p)
    {
        final TransportMode transport = TransportMode.valueOf(get(p, "transport", "IPC").toUpperCase(Locale.ROOT));
        return new DemoTraderConfig(
            transport,
            p.getProperty(PREFIX + "aeron.dir"),
            Boolean.parseBoolean(get(p, "aeron.embeddedMediaDriver", String.valueOf(transport == TransportMode.UDP))),
            get(p, "udp.engineHost", "localhost"),
            get(p, "udp.clientHost", "localhost"),
            Integer.parseInt(get(p, "udp.commandPort", String.valueOf(EngineChannels.COMMAND_PORT))),
            Integer.parseInt(get(p, "udp.executionControlPort", String.valueOf(EngineChannels.EXECUTION_CONTROL_PORT))),
            Integer.parseInt(get(p, "udp.marketDataControlPort",
                String.valueOf(EngineChannels.MARKET_DATA_CONTROL_PORT))),
            parseInstruments(get(p, "instruments", "1,EURUSD,108345,1,100")),
            Long.parseLong(get(p, "userBase", "1000")),
            Integer.parseInt(get(p, "users", "20")),
            Integer.parseInt(get(p, "ordersPerSecond", "50")),
            Long.parseLong(get(p, "maxOrders", "0")),
            Long.parseLong(get(p, "durationSeconds", "0")),
            Long.parseLong(get(p, "reportIntervalSeconds", "10")),
            Integer.parseInt(get(p, "spreadTicks", "20")),
            Integer.parseInt(get(p, "marketOrderPercent", "10")),
            Integer.parseInt(get(p, "cancelPercent", "15")),
            Integer.parseInt(get(p, "maxLots", "10")),
            Long.parseLong(get(p, "seed", "42")));
    }

    /**
     * Parse {@code id,symbol,basePrice,tickSize,lotSize} entries separated by {@code ;}.
     *
     * @param spec instrument specification.
     * @return instruments.
     */
    public static List<DemoInstrument> parseInstruments(final String spec)
    {
        final List<DemoInstrument> result = new ArrayList<>();
        for (final String entry : spec.split(";"))
        {
            if (entry.isBlank())
            {
                continue;
            }
            final String[] f = entry.trim().split(",");
            if (f.length != 5)
            {
                throw new IllegalArgumentException("instrument must be id,symbol,basePrice,tickSize,lotSize: " + entry);
            }
            result.add(new DemoInstrument(Integer.parseInt(f[0].trim()), f[1].trim(), Long.parseLong(f[2].trim()),
                Long.parseLong(f[3].trim()), Long.parseLong(f[4].trim())));
        }
        return result;
    }

    private static String get(final Properties p, final String key, final String defaultValue)
    {
        final String value = p.getProperty(PREFIX + key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }
}
