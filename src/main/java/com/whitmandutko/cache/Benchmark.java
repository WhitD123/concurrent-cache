package com.whitmandutko.cache;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple load client: fires concurrent PUT/GET requests at a running server
 * and prints throughput and latency.
 *
 * <p>Usage: {@code java -cp out com.whitmandutko.cache.Benchmark [host] [port] [threads] [opsPerThread]}
 * Defaults: ::1 (IPv6 loopback) 8080, 8 threads, 1000 ops per thread.
 * {@code localhost} works too on a normal machine — pass it as the first
 * argument. The default is ::1 rather than localhost because this project's
 * build VM blocks outbound IPv4 loopback TCP from Java processes, while
 * IPv6 loopback works; on a developer laptop either address works.
 *
 * <p>Each operation is a PUT followed by a GET of the same key (a realistic
 * write-then-read mix), so "ops" below counts individual HTTP requests.
 */
public final class Benchmark {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "::1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        int threads = args.length > 2 ? Integer.parseInt(args[2]) : 8;
        int opsPerThread = args.length > 3 ? Integer.parseInt(args[3]) : 1000;

        String base = "http://" + bracket(host) + ":" + port;
        waitForServer(base);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicLong successes = new AtomicLong();
        AtomicLong failures = new AtomicLong();
        AtomicLong totalLatencyNanos = new AtomicLong();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "bench-" + threadId + "-" + i;
                        long t0 = System.nanoTime();
                        try {
                            put(base, key, "value-" + i);
                            get(base, key);
                            successes.addAndGet(2);
                            totalLatencyNanos.addAndGet(System.nanoTime() - t0);
                        } catch (IOException e) {
                            failures.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        long wallStart = System.nanoTime();
        startGate.countDown(); // release all threads at once
        done.await();
        long wallNanos = System.nanoTime() - wallStart;
        pool.shutdownNow();

        long ok = successes.get();
        double seconds = wallNanos / 1_000_000_000.0;
        double throughput = ok / seconds;
        double avgLatencyMs = ok == 0 ? 0 : (totalLatencyNanos.get() / 1_000_000.0) / (ok / 2.0);

        System.out.println("=== Benchmark results ===");
        System.out.println("Threads:            " + threads);
        System.out.println("Ops per thread:     " + opsPerThread + " PUT+GET pairs");
        System.out.println("Successful requests:" + ok);
        System.out.println("Failed requests:    " + failures.get());
        System.out.printf("Wall time:          %.3f s%n", seconds);
        System.out.printf("Throughput:         %.0f req/s%n", throughput);
        System.out.printf("Avg latency:        %.3f ms per PUT+GET pair%n", avgLatencyMs);
    }

    /** IPv6 literals need brackets in a URL; hostnames and IPv4 do not. */
    private static String bracket(String host) {
        return host.contains(":") && !host.startsWith("[") ? "[" + host + "]" : host;
    }

    private static void waitForServer(String base) throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            try {
                HttpURLConnection c = (HttpURLConnection) new URL(base + "/health").openConnection();
                c.setConnectTimeout(500);
                c.setReadTimeout(500);
                if (c.getResponseCode() == 200) {
                    return;
                }
            } catch (IOException ignored) {
                // Not up yet.
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("Server did not come up at " + base + "/health — start it first (./run.sh)");
    }

    private static void put(String base, String key, String value) throws IOException {
        HttpURLConnection c = (HttpURLConnection) new URL(base + "/cache/" + key).openConnection();
        c.setRequestMethod("PUT");
        c.setDoOutput(true);
        c.setRequestProperty("Content-Type", "application/json");
        byte[] body = ("{\"value\":\"" + value + "\",\"ttlSeconds\":300}").getBytes(StandardCharsets.UTF_8);
        c.setFixedLengthStreamingMode(body.length);
        try (OutputStream out = c.getOutputStream()) {
            out.write(body);
        }
        int code = c.getResponseCode();
        c.disconnect();
        if (code != 200) {
            throw new IOException("PUT failed: HTTP " + code);
        }
    }

    private static String get(String base, String key) throws IOException {
        HttpURLConnection c = (HttpURLConnection) new URL(base + "/cache/" + key).openConnection();
        c.setRequestMethod("GET");
        int code = c.getResponseCode();
        String body = code == 200
                ? new String(c.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
                : "";
        c.disconnect();
        if (code != 200) {
            throw new IOException("GET failed: HTTP " + code);
        }
        return body;
    }
}
