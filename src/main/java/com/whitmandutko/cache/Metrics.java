package com.whitmandutko.cache;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe per-operation counters for the cache.
 *
 * <p>All counters are {@link AtomicLong}s so request threads can record
 * without taking the cache lock — metrics must never become a contention
 * point for the data path.
 */
public final class Metrics {
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private final AtomicLong expirations = new AtomicLong();
    private final AtomicLong totalLatencyNanos = new AtomicLong();
    private final AtomicLong operations = new AtomicLong();
    private final long startedAtMillis = System.currentTimeMillis();

    public void recordHit(long latencyNanos) {
        hits.incrementAndGet();
        recordLatency(latencyNanos);
    }

    public void recordMiss(long latencyNanos) {
        misses.incrementAndGet();
        recordLatency(latencyNanos);
    }

    public void recordPut(long latencyNanos) {
        puts.incrementAndGet();
        recordLatency(latencyNanos);
    }

    public void recordDelete(long latencyNanos) {
        deletes.incrementAndGet();
        recordLatency(latencyNanos);
    }

    public void recordEviction() { evictions.incrementAndGet(); }
    public void recordExpiration() { expirations.incrementAndGet(); }
    public void recordExpirations(long n) { expirations.addAndGet(n); }

    private void recordLatency(long latencyNanos) {
        totalLatencyNanos.addAndGet(latencyNanos);
        operations.incrementAndGet();
    }

    public long hits() { return hits.get(); }
    public long misses() { return misses.get(); }
    public long puts() { return puts.get(); }
    public long deletes() { return deletes.get(); }
    public long evictions() { return evictions.get(); }
    public long expirations() { return expirations.get(); }
    public long operations() { return operations.get(); }

    public double hitRate() {
        long h = hits.get();
        long m = misses.get();
        long total = h + m;
        return total == 0 ? 0.0 : (double) h / total;
    }

    /** Mean latency across all recorded operations, in milliseconds. */
    public double avgLatencyMillis() {
        long ops = operations.get();
        return ops == 0 ? 0.0 : totalLatencyNanos.get() / 1_000_000.0 / ops;
    }

    public long uptimeSeconds() {
        return (System.currentTimeMillis() - startedAtMillis) / 1000L;
    }

    /** Minimal hand-rolled JSON — no dependency needed for six numbers. */
    public String toJson(int size, int capacity) {
        return "{"
                + "\"hits\":" + hits.get()
                + ",\"misses\":" + misses.get()
                + ",\"puts\":" + puts.get()
                + ",\"deletes\":" + deletes.get()
                + ",\"evictions\":" + evictions.get()
                + ",\"expirations\":" + expirations.get()
                + ",\"operations\":" + operations.get()
                + ",\"hitRate\":" + String.format("%.4f", hitRate())
                + ",\"avgLatencyMs\":" + String.format("%.4f", avgLatencyMillis())
                + ",\"size\":" + size
                + ",\"capacity\":" + capacity
                + ",\"uptimeSeconds\":" + uptimeSeconds()
                + "}";
    }
}
