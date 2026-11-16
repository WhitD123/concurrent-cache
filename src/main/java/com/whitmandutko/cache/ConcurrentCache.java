package com.whitmandutko.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe, bounded, in-memory key-value cache with LRU eviction and TTL.
 *
 * <h2>Concurrency design</h2>
 * A single {@link ReentrantLock} guards the map. That is a deliberate
 * trade-off (see README): the map operations are O(1) and very short, so a
 * single lock is simpler and — at this scale — faster than sharding, and it
 * makes the LRU ordering (which is inherently global) correct by
 * construction. A {@code ConcurrentHashMap} alone cannot maintain LRU order,
 * which is why the map itself is a plain {@link LinkedHashMap} in
 * access-order mode, with all access funnelled through the lock.
 *
 * <h2>LRU</h2>
 * {@link LinkedHashMap} is constructed with {@code accessOrder=true}, so
 * every {@code get} moves the entry to the tail. The head is therefore
 * always the least-recently-used entry, and eviction is just "remove the
 * head while size exceeds capacity".
 */
public final class ConcurrentCache {

    private final int maxCapacity;
    private final Metrics metrics;

    // accessOrder=true: iteration order is least-recently-used first.
    private final LinkedHashMap<String, CacheEntry> map;
    private final ReentrantLock lock = new ReentrantLock();

    private ScheduledExecutorService sweeper;

    public ConcurrentCache(int maxCapacity, Metrics metrics) {
        if (maxCapacity <= 0) {
            throw new IllegalArgumentException("maxCapacity must be > 0");
        }
        this.maxCapacity = maxCapacity;
        this.metrics = metrics;
        this.map = new LinkedHashMap<>(16, 0.75f, true);
    }

    public int maxCapacity() { return maxCapacity; }
    public Metrics metrics() { return metrics; }

    // ---- core operations -------------------------------------------------

    public void put(String key, String value, long ttlSeconds) {
        long start = System.nanoTime();
        lock.lock();
        try {
            map.put(key, new CacheEntry(key, value, ttlSeconds));
            evictIfNeeded();
        } finally {
            lock.unlock();
        }
        metrics.recordPut(System.nanoTime() - start);
    }

    /** Returns the value, or {@code null} on a miss or an expired entry. */
    public String get(String key) {
        long start = System.nanoTime();
        lock.lock();
        try {
            CacheEntry entry = map.get(key); // access-order: refreshes LRU position
            if (entry == null) {
                metrics.recordMiss(System.nanoTime() - start);
                return null;
            }
            if (entry.isExpired()) {
                map.remove(key);
                metrics.recordExpiration();
                metrics.recordMiss(System.nanoTime() - start);
                return null;
            }
            metrics.recordHit(System.nanoTime() - start);
            return entry.value();
        } finally {
            lock.unlock();
        }
    }

    /** Returns the entry (with TTL metadata), or {@code null} if absent/expired. */
    public CacheEntry getEntry(String key) {
        long start = System.nanoTime();
        lock.lock();
        try {
            CacheEntry entry = map.get(key);
            if (entry == null || entry.isExpired()) {
                if (entry != null) {
                    map.remove(key);
                    metrics.recordExpiration();
                }
                metrics.recordMiss(System.nanoTime() - start);
                return null;
            }
            metrics.recordHit(System.nanoTime() - start);
            return entry;
        } finally {
            lock.unlock();
        }
    }

    public boolean delete(String key) {
        long start = System.nanoTime();
        lock.lock();
        try {
            boolean removed = map.remove(key) != null;
            metrics.recordDelete(System.nanoTime() - start);
            return removed;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return map.size();
        } finally {
            lock.unlock();
        }
    }

    /** Snapshot of all live entries, in LRU order (least-recent first). */
    public List<CacheEntry> entries() {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            List<CacheEntry> live = new ArrayList<>();
            for (CacheEntry e : map.values()) {
                if (!e.isExpiredAt(now)) {
                    live.add(e);
                }
            }
            return live;
        } finally {
            lock.unlock();
        }
    }

    /** Used by persistence: bulk-load entries without counting them as puts. */
    public void load(List<CacheEntry> entries) {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            for (CacheEntry e : entries) {
                if (!e.isExpiredAt(now)) {
                    map.put(e.key(), e);
                }
            }
            evictIfNeeded();
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        lock.lock();
        try {
            map.clear();
        } finally {
            lock.unlock();
        }
    }

    // ---- eviction / expiry ------------------------------------------------

    /** Must be called with {@link #lock} held. Removes LRU entries until in-bounds. */
    private void evictIfNeeded() {
        while (map.size() > maxCapacity) {
            Iterator<Map.Entry<String, CacheEntry>> it = map.entrySet().iterator();
            if (!it.hasNext()) {
                break;
            }
            it.next();
            it.remove();
            metrics.recordEviction();
        }
    }

    /**
     * Removes every expired entry. Returns how many were removed.
     * Called by the background sweeper and directly by tests.
     */
    public int sweepExpired() {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            int removed = 0;
            Iterator<Map.Entry<String, CacheEntry>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                if (it.next().getValue().isExpiredAt(now)) {
                    it.remove();
                    removed++;
                }
            }
            if (removed > 0) {
                metrics.recordExpirations(removed);
            }
            return removed;
        } finally {
            lock.unlock();
        }
    }

    /** Starts a daemon thread that sweeps expired entries every {@code intervalSeconds}. */
    public void startSweeper(long intervalSeconds) {
        if (sweeper != null && !sweeper.isShutdown()) {
            return;
        }
        sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-sweeper");
            t.setDaemon(true);
            return t;
        });
        sweeper.scheduleAtFixedRate(
                this::sweepExpired, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    public void stopSweeper() {
        if (sweeper != null) {
            sweeper.shutdownNow();
        }
    }
}
