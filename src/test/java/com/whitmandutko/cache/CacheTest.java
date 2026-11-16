package com.whitmandutko.cache;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Plain-Java test harness — no JUnit, no dependencies.
 * Run with {@code ./run_tests.sh}. Exits 0 only if every test passes.
 */
public final class CacheTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) throws Exception {
        testBasicPutGet();
        testMissReturnsNull();
        testDelete();
        testLruEviction();
        testLruAccessRefreshesOrder();
        testTtlExpiryOnGet();
        testSweeperRemovesExpired();
        testConcurrentPutsNoLoss();
        testConcurrentMixedWorkload();
        testMetricsCorrectness();
        testPersistenceRoundTrip();
        testPersistenceSkipsExpired();
        testOverwriteResetsTtl();

        System.out.println();
        System.out.println("Tests passed: " + passed + ", failed: " + failed);
        if (failed > 0) {
            System.out.println("RESULT: FAIL");
            System.exit(1);
        }
        System.out.println("RESULT: PASS");
    }

    // ---- helpers ------------------------------------------------------------

    private static void check(boolean condition, String name) {
        if (condition) {
            passed++;
            System.out.println("  PASS: " + name);
        } else {
            failed++;
            System.out.println("  FAIL: " + name);
        }
    }

    private static void checkEq(Object expected, Object actual, String name) {
        check(expected == null ? actual == null : expected.equals(actual),
                name + " (expected=" + expected + ", actual=" + actual + ")");
    }

    // ---- tests ---------------------------------------------------------------

    private static void testBasicPutGet() {
        System.out.println("testBasicPutGet");
        ConcurrentCache c = new ConcurrentCache(100, new Metrics());
        c.put("a", "1", 0);
        checkEq("1", c.get("a"), "get returns stored value");
        checkEq(1, c.size(), "size is 1 after one put");
    }

    private static void testMissReturnsNull() {
        System.out.println("testMissReturnsNull");
        ConcurrentCache c = new ConcurrentCache(100, new Metrics());
        checkEq(null, c.get("nope"), "missing key returns null");
    }

    private static void testDelete() {
        System.out.println("testDelete");
        ConcurrentCache c = new ConcurrentCache(100, new Metrics());
        c.put("a", "1", 0);
        check(c.delete("a"), "delete of existing key returns true");
        checkEq(null, c.get("a"), "deleted key is gone");
        check(!c.delete("a"), "delete of absent key returns false");
    }

    private static void testLruEviction() {
        System.out.println("testLruEviction");
        ConcurrentCache c = new ConcurrentCache(3, new Metrics());
        c.put("a", "1", 0);
        c.put("b", "2", 0);
        c.put("c", "3", 0);
        c.put("d", "4", 0); // capacity 3: "a" (least recently used) must go
        checkEq(null, c.get("a"), "LRU entry evicted when over capacity");
        checkEq("2", c.get("b"), "other entries survive eviction");
        checkEq("4", c.get("d"), "newest entry present");
        checkEq(3, c.size(), "size stays at capacity");
        checkEq(1L, c.metrics().evictions(), "one eviction counted");
    }

    private static void testLruAccessRefreshesOrder() {
        System.out.println("testLruAccessRefreshesOrder");
        ConcurrentCache c = new ConcurrentCache(3, new Metrics());
        c.put("a", "1", 0);
        c.put("b", "2", 0);
        c.put("c", "3", 0);
        c.get("a"); // "a" is now most recently used; "b" is LRU
        c.put("d", "4", 0);
        checkEq(null, c.get("b"), "accessed entry is protected; LRU entry evicted instead");
        checkEq("1", c.get("a"), "recently accessed entry survives");
    }

    private static void testTtlExpiryOnGet() throws Exception {
        System.out.println("testTtlExpiryOnGet");
        ConcurrentCache c = new ConcurrentCache(100, new Metrics());
        c.put("short", "x", 1); // 1 second TTL
        checkEq("x", c.get("short"), "entry readable before TTL");
        Thread.sleep(1200);
        checkEq(null, c.get("short"), "entry gone after TTL");
        checkEq(1L, c.metrics().expirations(), "expiration counted on get");
    }

    private static void testSweeperRemovesExpired() throws Exception {
        System.out.println("testSweeperRemovesExpired");
        ConcurrentCache c = new ConcurrentCache(100, new Metrics());
        c.put("e1", "1", 1);
        c.put("e2", "2", 1);
        c.put("keep", "3", 0);
        Thread.sleep(1200);
        int removed = c.sweepExpired();
        checkEq(2, removed, "sweeper removes both expired entries");
        checkEq(1, c.size(), "only the non-expiring entry remains");
        checkEq("3", c.get("keep"), "non-expiring entry untouched by sweep");
    }

    private static void testConcurrentPutsNoLoss() throws Exception {
        System.out.println("testConcurrentPutsNoLoss (10 threads x 1000 puts)");
        int threads = 10;
        int perThread = 1000;
        ConcurrentCache c = new ConcurrentCache(threads * perThread + 100, new Metrics());
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        c.put("t" + id + "-" + i, "v" + i, 0);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        done.await();
        pool.shutdownNow();
        checkEq(0, errors.get(), "no thread threw an exception");
        checkEq(threads * perThread, c.size(), "all 10,000 entries present — none lost");
        // Spot-check a sample across threads; a lost/corrupt write shows up here.
        boolean allOk = true;
        for (int t = 0; t < threads && allOk; t++) {
            for (int i = 0; i < perThread; i += 137) {
                if (!("v" + i).equals(c.get("t" + t + "-" + i))) {
                    allOk = false;
                    break;
                }
            }
        }
        check(allOk, "sampled entries have correct values (no corruption)");
    }

    private static void testConcurrentMixedWorkload() throws Exception {
        System.out.println("testConcurrentMixedWorkload (8 threads, shared keys)");
        ConcurrentCache c = new ConcurrentCache(1000, new Metrics());
        for (int i = 0; i < 100; i++) {
            c.put("shared-" + i, "init", 0);
        }
        int threads = 8;
        int ops = 2000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < ops; i++) {
                        String key = "shared-" + (i % 100);
                        if (i % 3 == 0) {
                            c.put(key, "v" + i, 0);
                        } else {
                            c.get(key); // must never throw / return garbage
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        done.await();
        pool.shutdownNow();
        checkEq(0, errors.get(), "no thread threw under mixed read/write load");
        checkEq(100, c.size(), "shared-key set intact after mixed workload");
        Set<String> values = new HashSet<>();
        for (CacheEntry e : c.entries()) {
            values.add(e.value());
        }
        check(!values.contains(null), "no null values after concurrent writes");
    }

    private static void testMetricsCorrectness() {
        System.out.println("testMetricsCorrectness");
        Metrics m = new Metrics();
        ConcurrentCache c = new ConcurrentCache(100, m);
        c.put("a", "1", 0);
        c.put("b", "2", 0);
        c.get("a");      // hit
        c.get("a");      // hit
        c.get("zzz");    // miss
        checkEq(2L, m.hits(), "hits counted");
        checkEq(1L, m.misses(), "misses counted");
        checkEq(2L, m.puts(), "puts counted");
        check(Math.abs(m.hitRate() - 2.0 / 3.0) < 0.0001, "hit rate = 2/3");
        check(m.avgLatencyMillis() >= 0, "avg latency is non-negative");
        check(m.operations() == 5, "operations = hits + misses + puts");
    }

    private static void testPersistenceRoundTrip() throws Exception {
        System.out.println("testPersistenceRoundTrip");
        Path tmp = Files.createTempFile("cache-test-", ".txt");
        try {
            ConcurrentCache c = new ConcurrentCache(100, new Metrics());
            c.put("hello", "world", 0);
            c.put("tab\tkey", "va\nlue", 3600); // awkward chars must survive
            c.put("uni", "héllo→", 0);
            Persistence.save(tmp, c.entries());

            ConcurrentCache c2 = new ConcurrentCache(100, new Metrics());
            c2.load(Persistence.load(tmp));
            checkEq("world", c2.get("hello"), "plain entry survives save/load");
            checkEq("va\nlue", c2.get("tab\tkey"), "entry with tab/newline survives save/load");
            checkEq("héllo→", c2.get("uni"), "unicode entry survives save/load");
            checkEq(3, c2.size(), "all entries restored");
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private static void testPersistenceSkipsExpired() throws Exception {
        System.out.println("testPersistenceSkipsExpired");
        Path tmp = Files.createTempFile("cache-test-", ".txt");
        try {
            ConcurrentCache c = new ConcurrentCache(100, new Metrics());
            c.put("dead", "1", 1);
            c.put("live", "2", 0);
            Thread.sleep(1200);
            Persistence.save(tmp, c.entries()); // entries() already filters expired
            ConcurrentCache c2 = new ConcurrentCache(100, new Metrics());
            c2.load(Persistence.load(tmp));
            checkEq(null, c2.get("dead"), "expired entry not restored");
            checkEq("2", c2.get("live"), "live entry restored");
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private static void testOverwriteResetsTtl() throws Exception {
        System.out.println("testOverwriteResetsTtl");
        ConcurrentCache c = new ConcurrentCache(100, new Metrics());
        c.put("k", "old", 1);
        Thread.sleep(600);
        c.put("k", "new", 60); // overwrite with a fresh TTL
        Thread.sleep(700); // past the original 1s deadline
        checkEq("new", c.get("k"), "overwrite resets the TTL deadline");
    }
}
