package com.flashexchange.engine.ops;

import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EngineHttpServerTest
{
    final HttpClient http = HttpClient.newHttpClient();

    HttpResponse<String> send(final String method, final int port, final String path) throws Exception
    {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
            .method(method, HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void healthReflectsLivenessAndStatusIsJson() throws Exception
    {
        final AtomicBoolean up = new AtomicBoolean(true);
        final Map<String, Object> status = new LinkedHashMap<>();
        status.put("name", "engine \"x\"");
        status.put("count", 3);
        status.put("ok", true);
        status.put("list", List.of(1, "a"));
        try (EngineHttpServer server = new EngineHttpServer("127.0.0.1", 0, up::get, () -> status))
        {
            final int port = server.port();
            assertTrue(port > 0);
            HttpResponse<String> r = send("GET", port, "/health");
            assertEquals(200, r.statusCode());
            assertEquals("{\"status\":\"UP\"}", r.body());
            assertEquals("application/json", r.headers().firstValue("Content-Type").orElseThrow());

            up.set(false);
            r = send("GET", port, "/health");
            assertEquals(503, r.statusCode());
            assertEquals("{\"status\":\"DOWN\"}", r.body());

            r = send("GET", port, "/status");
            assertEquals(200, r.statusCode());
            assertEquals("{\"name\":\"engine \\\"x\\\"\",\"count\":3,\"ok\":true,\"list\":[1,\"a\"]}", r.body());

            r = send("POST", port, "/status");
            assertEquals(405, r.statusCode());
            assertEquals("GET", r.headers().firstValue("Allow").orElseThrow());
        }
    }

    @Test
    void bindFailureIsReported()
    {
        try (EngineHttpServer first = new EngineHttpServer("127.0.0.1", 0, () -> true, Map::of))
        {
            assertThrows(UncheckedIOException.class,
                () -> new EngineHttpServer("127.0.0.1", first.port(), () -> true, Map::of));
        }
    }

    @Test
    void jsonEscapesAndHandlesNulls()
    {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("s", "a\\b\n\r\t\u0001");
        m.put("n", null);
        m.put("d", 1.5);
        m.put("o", new Object()
        {
            @Override
            public String toString()
            {
                return "obj";
            }
        });
        assertEquals("{\"s\":\"a\\\\b\\n\\r\\t\\u0001\",\"n\":null,\"d\":1.5,\"o\":\"obj\"}", JsonWriter.toJson(m));
        assertEquals("[]", JsonWriter.toJson(List.of()));
        assertEquals("{}", JsonWriter.toJson(Map.of()));
    }
}
