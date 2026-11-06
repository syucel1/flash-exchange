package com.flashexchange.engine.ops;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Small operational HTTP endpoint used by container health checks and operators. Runs on its own daemon thread
 * using the JDK's built-in HTTP server, far away from the matching threads.
 * <ul>
 *   <li>{@code GET /health} returns 200 {@code {"status":"UP"}} or 503 {@code {"status":"DOWN"}}.</li>
 *   <li>{@code GET /status} returns a JSON document with configuration and counters.</li>
 * </ul>
 */
public final class EngineHttpServer implements AutoCloseable
{
    private final HttpServer server;
    private final ExecutorService executor;

    /**
     * Start the server.
     *
     * @param host    bind address (e.g. {@code 0.0.0.0}).
     * @param port    port, or 0 for an ephemeral port.
     * @param healthy liveness check.
     * @param status  status document supplier.
     */
    public EngineHttpServer(
        final String host, final int port, final BooleanSupplier healthy, final Supplier<Map<String, Object>> status)
    {
        try
        {
            server = HttpServer.create(new InetSocketAddress(host, port), 16);
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException("cannot bind http server to " + host + ":" + port, ex);
        }
        executor = Executors.newSingleThreadExecutor(r ->
        {
            final Thread thread = new Thread(r, "engine-http");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.createContext("/health", exchange ->
        {
            final boolean up = healthy.getAsBoolean();
            respond(exchange, up ? 200 : 503, up ? "{\"status\":\"UP\"}" : "{\"status\":\"DOWN\"}");
        });
        server.createContext("/status", exchange -> respond(exchange, 200, JsonWriter.toJson(status.get())));
        server.start();
    }

    private static void respond(final HttpExchange exchange, final int code, final String body) throws IOException
    {
        try (exchange)
        {
            if (!"GET".equals(exchange.getRequestMethod()))
            {
                exchange.getResponseHeaders().add("Allow", "GET");
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(code, bytes.length);
            try (OutputStream out = exchange.getResponseBody())
            {
                out.write(bytes);
            }
        }
    }

    /**
     * Port the server is bound to.
     *
     * @return port.
     */
    public int port()
    {
        return server.getAddress().getPort();
    }

    /**
     * Stop the server.
     */
    @Override
    public void close()
    {
        server.stop(0);
        executor.shutdownNow();
    }
}
