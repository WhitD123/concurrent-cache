package com.whitmandutko.cache;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * HTTP front-end for {@link ConcurrentCache}, built on the JDK's own
 * {@code com.sun.net.httpserver.HttpServer} — no external dependencies.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>{@code GET  /health} — {@code {"status":"ok"}}</li>
 *   <li>{@code GET  /metrics} — counters, hit rate, avg latency, size</li>
 *   <li>{@code GET  /cache} — all live entries as a JSON object</li>
 *   <li>{@code PUT  /cache/{key}} — body {@code {"value":"...","ttlSeconds":60}}</li>
 *   <li>{@code GET  /cache/{key}} — {@code {"key":"...","value":"...","ttlSeconds":...}}</li>
 *   <li>{@code DELETE /cache/{key}} — 204 on success, 404 if absent</li>
 * </ul>
 *
 * <p>Usage: {@code java -cp out com.whitmandutko.cache.CacheServer [port] [capacity] [snapshotPath]}
 * Defaults: port 8080, capacity 1000, snapshot {@code data/snapshot.txt}.
 * The snapshot is loaded on startup and saved on shutdown (SIGINT/SIGTERM).
 */
public final class CacheServer {

    private static final Pattern VALUE_PATTERN =
            Pattern.compile("\"value\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"");
    private static final Pattern TTL_PATTERN =
            Pattern.compile("\"ttlSeconds\"\\s*:\\s*(-?\\d+)");

    private final ConcurrentCache cache;
    private final Path snapshotPath;
    private HttpServer server;
    private ExecutorService executor;

    public CacheServer(ConcurrentCache cache, Path snapshotPath) {
        this.cache = cache;
        this.snapshotPath = snapshotPath;
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        // Fixed pool: bounds concurrency so a burst of clients cannot spawn
        // unbounded threads. Sized to 2x available cores — a good default for
        // a mostly I/O-bound HTTP workload (see README).
        int poolSize = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
        executor = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "cache-worker");
            t.setDaemon(false);
            return t;
        });
        server.setExecutor(executor);
        server.createContext("/health", this::handleHealth);
        server.createContext("/metrics", this::handleMetrics);
        server.createContext("/cache", this::handleCache);
        server.createContext("/", this::handleNotFound);
        server.start();
        System.out.println("ConcurrentCache listening on http://localhost:" + port
                + " (worker pool: " + poolSize + " threads, capacity: "
                + cache.maxCapacity() + ")");
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
        cache.stopSweeper();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    // ---- handlers ----------------------------------------------------------

    private void handleHealth(HttpExchange ex) throws IOException {
        sendJson(ex, 200, "{\"status\":\"ok\"}");
    }

    private void handleMetrics(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, "{\"error\":\"method not allowed\"}");
            return;
        }
        sendJson(ex, 200, cache.metrics().toJson(cache.size(), cache.maxCapacity()));
    }

    private void handleCache(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod().toUpperCase();
        String path = ex.getRequestURI().getPath();

        if ("/cache".equals(path) || "/cache/".equals(path)) {
            if ("GET".equals(method)) {
                sendJson(ex, 200, listJson());
            } else {
                sendJson(ex, 405, "{\"error\":\"method not allowed\"}");
            }
            return;
        }

        if (!path.startsWith("/cache/")) {
            sendJson(ex, 404, "{\"error\":\"not found\"}");
            return;
        }
        String key = URLDecoder.decode(path.substring("/cache/".length()), StandardCharsets.UTF_8);
        if (key.isEmpty()) {
            sendJson(ex, 400, "{\"error\":\"empty key\"}");
            return;
        }

        switch (method) {
            case "GET" -> {
                CacheEntry entry = cache.getEntry(key);
                if (entry == null) {
                    sendJson(ex, 404, "{\"error\":\"key not found\"}");
                } else {
                    sendJson(ex, 200, "{\"key\":" + jsonString(key)
                            + ",\"value\":" + jsonString(entry.value())
                            + ",\"ttlSeconds\":" + entry.ttlRemainingSeconds() + "}");
                }
            }
            case "PUT" -> {
                String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                String value = parseValue(body);
                if (value == null) {
                    sendJson(ex, 400, "{\"error\":\"body must be JSON with a string \\\"value\\\"\"}");
                    return;
                }
                long ttl = parseTtl(body);
                if (ttl < 0) {
                    sendJson(ex, 400, "{\"error\":\"ttlSeconds must be >= 0\"}");
                    return;
                }
                cache.put(key, value, ttl);
                sendJson(ex, 200, "{\"key\":" + jsonString(key)
                        + ",\"value\":" + jsonString(value)
                        + ",\"ttlSeconds\":" + ttl + "}");
            }
            case "DELETE" -> {
                if (cache.delete(key)) {
                    sendJson(ex, 200, "{\"deleted\":true,\"key\":" + jsonString(key) + "}");
                } else {
                    sendJson(ex, 404, "{\"error\":\"key not found\"}");
                }
            }
            default -> sendJson(ex, 405, "{\"error\":\"method not allowed\"}");
        }
    }

    private void handleNotFound(HttpExchange ex) throws IOException {
        sendJson(ex, 404, "{\"error\":\"not found\"}");
    }

    // ---- helpers -----------------------------------------------------------

    private String listJson() {
        List<CacheEntry> entries = cache.entries();
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < entries.size(); i++) {
            CacheEntry e = entries.get(i);
            if (i > 0) sb.append(',');
            sb.append(jsonString(e.key())).append(':').append(jsonString(e.value()));
        }
        sb.append('}');
        return sb.toString();
    }

    private static void sendJson(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    /** Extracts the "value" string from a minimal JSON body (handles \" and \\ escapes). */
    static String parseValue(String body) {
        Matcher m = VALUE_PATTERN.matcher(body);
        if (!m.find()) {
            return null;
        }
        return m.group(1).replace("\\\"", "\"").replace("\\\\", "\\");
    }

    static long parseTtl(String body) {
        Matcher m = TTL_PATTERN.matcher(body);
        if (!m.find()) {
            return 0; // no TTL given: never expires
        }
        try {
            return Long.parseLong(m.group(1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** Minimal JSON string escaping — enough for the values this API stores. */
    static String jsonString(String s) {
        return "\"" + s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t") + "\"";
    }

    // ---- main ---------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        int capacity = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
        Path snapshot = Path.of(args.length > 2 ? args[2] : "data/snapshot.txt");

        Metrics metrics = new Metrics();
        ConcurrentCache cache = new ConcurrentCache(capacity, metrics);

        // Restore the previous snapshot, if one exists.
        List<CacheEntry> restored = Persistence.load(snapshot);
        cache.load(restored);
        System.out.println("Restored " + restored.size() + " entries from " + snapshot);

        cache.startSweeper(5); // expire stale entries every 5 seconds
        CacheServer server = new CacheServer(cache, snapshot);
        server.start(port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down — saving snapshot...");
            try {
                Persistence.save(snapshot, cache.entries());
                System.out.println("Saved " + cache.size() + " entries to " + snapshot);
            } catch (IOException e) {
                System.err.println("Failed to save snapshot: " + e.getMessage());
            }
            server.stop();
        }));

        System.out.println("Press Ctrl+C to stop.");
        // Keep the main thread alive; the server runs on its own threads.
        Object lock = new Object();
        synchronized (lock) {
            lock.wait();
        }
    }
}
