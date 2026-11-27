package com.flashexchange.engine.ha;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Arbiter configuration (properties prefixed {@code flash.arbiter.}).
 *
 * @param listenHost          host the heartbeat endpoint binds to (stream 9001).
 * @param listenPort          heartbeat port.
 * @param nodes               engine nodes in promotion preference order.
 * @param leaseDurationMs     lease granted to the primary; renewed every third of it.
 * @param primaryTimeoutMs    heartbeat silence after which the primary is considered dead.
 * @param leaseGuardMs        extra wait after the last lease grant before promoting (covers delivery delay).
 * @param startupGraceMs      how long a fresh cluster waits for the preferred node before electing another.
 * @param stateFile           where the epoch and primary are persisted.
 * @param aeronDirectory      Aeron directory ({@code null} = default).
 * @param embeddedMediaDriver whether to launch a media driver in-process.
 * @param httpHost            host of the {@code /health} and {@code /status} endpoint.
 * @param httpPort            port of that endpoint (-1 disabled, 0 ephemeral).
 */
public record ArbiterConfig(
    String listenHost,
    int listenPort,
    List<Node> nodes,
    long leaseDurationMs,
    long primaryTimeoutMs,
    long leaseGuardMs,
    long startupGraceMs,
    String stateFile,
    String aeronDirectory,
    boolean embeddedMediaDriver,
    String httpHost,
    int httpPort)
{
    /** Property prefix. */
    public static final String PREFIX = "flash.arbiter.";

    /**
     * An engine node as seen by the arbiter.
     *
     * @param id          node id.
     * @param host        host of the node's control endpoint.
     * @param controlPort port of the node's control endpoint (stream 9002).
     */
    public record Node(int id, String host, int controlPort)
    {
    }

    /**
     * Validate.
     */
    public ArbiterConfig
    {
        nodes = List.copyOf(nodes);
        if (nodes.size() < 2)
        {
            throw new IllegalArgumentException("the arbiter needs at least two nodes");
        }
        if (leaseDurationMs < 3 || primaryTimeoutMs < 1 || leaseGuardMs < 0 || startupGraceMs < 0)
        {
            throw new IllegalArgumentException("invalid arbiter timing");
        }
    }

    /**
     * Parse {@code id=host:port} entries separated by commas.
     *
     * @param spec node specification.
     * @return nodes.
     */
    public static List<Node> parseNodes(final String spec)
    {
        final List<Node> nodes = new ArrayList<>();
        for (final String entry : spec.split(","))
        {
            final String trimmed = entry.trim();
            if (trimmed.isEmpty())
            {
                continue;
            }
            final int eq = trimmed.indexOf('=');
            final int colon = trimmed.lastIndexOf(':');
            if (eq < 1 || colon < eq)
            {
                throw new IllegalArgumentException("invalid node entry (id=host:port): " + trimmed);
            }
            nodes.add(new Node(Integer.parseInt(trimmed.substring(0, eq)), trimmed.substring(eq + 1, colon),
                Integer.parseInt(trimmed.substring(colon + 1))));
        }
        return nodes;
    }

    /**
     * Build from properties.
     *
     * @param p properties.
     * @return configuration.
     */
    public static ArbiterConfig fromProperties(final Properties p)
    {
        return new ArbiterConfig(
            get(p, "listen.host", "0.0.0.0"),
            Integer.parseInt(get(p, "listen.port", "40901")),
            parseNodes(get(p, "nodes", "1=localhost:40113,2=localhost:40213")),
            Long.parseLong(get(p, "leaseDurationMs", "300")),
            Long.parseLong(get(p, "primaryTimeoutMs", "250")),
            Long.parseLong(get(p, "leaseGuardMs", "100")),
            Long.parseLong(get(p, "startupGraceMs", "2000")),
            get(p, "state.file", "arbiter.state"),
            p.getProperty(PREFIX + "aeron.dir"),
            Boolean.parseBoolean(get(p, "aeron.embeddedMediaDriver", "true")),
            get(p, "http.host", "0.0.0.0"),
            Integer.parseInt(get(p, "http.port", "-1")));
    }

    /**
     * Load from an optional file overlaid with {@code flash.arbiter.*} system properties.
     *
     * @param file properties file or {@code null}.
     * @return configuration.
     * @throws IOException if the file cannot be read.
     */
    public static ArbiterConfig load(final Path file) throws IOException
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

    private static String get(final Properties p, final String key, final String defaultValue)
    {
        final String value = p.getProperty(PREFIX + key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }
}
