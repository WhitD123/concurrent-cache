package com.whitmandutko.cache;

/**
 * One entry in the cache.
 *
 * <p>An entry is immutable except for its access bookkeeping, which lives in
 * the owning {@link ConcurrentCache}'s access-ordered map. {@code expiresAtMillis}
 * is an absolute epoch-millis deadline, or 0 for "never expires".
 */
public final class CacheEntry {
    private final String key;
    private final String value;
    private final long expiresAtMillis; // 0 = no expiry
    private final long createdAtMillis;

    public CacheEntry(String key, String value, long ttlSeconds) {
        this(key, value,
                ttlSeconds > 0
                        ? System.currentTimeMillis() + ttlSeconds * 1000L
                        : 0L,
                System.currentTimeMillis());
    }

    public CacheEntry(String key, String value, long expiresAtMillis, long createdAtMillis) {
        this.key = key;
        this.value = value;
        this.expiresAtMillis = expiresAtMillis;
        this.createdAtMillis = createdAtMillis;
    }

    public String key() { return key; }
    public String value() { return value; }
    public long expiresAtMillis() { return expiresAtMillis; }
    public long createdAtMillis() { return createdAtMillis; }

    /** Remaining TTL in seconds, or -1 for "never expires". */
    public long ttlRemainingSeconds() {
        if (expiresAtMillis == 0) return -1;
        long remaining = (expiresAtMillis - System.currentTimeMillis()) / 1000L;
        return Math.max(0, remaining);
    }

    public boolean isExpired() {
        return expiresAtMillis != 0 && System.currentTimeMillis() >= expiresAtMillis;
    }

    public boolean isExpiredAt(long nowMillis) {
        return expiresAtMillis != 0 && nowMillis >= expiresAtMillis;
    }
}
