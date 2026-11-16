package com.whitmandutko.cache;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Snapshot persistence: the whole cache is written to a plain-text file
 * on shutdown and reloaded on startup, so a restart does not lose data.
 *
 * <p>Format — one entry per line, tab-separated:
 * <pre>
 * base64(key) \t base64(value) \t expiresAtMillis \t createdAtMillis
 * </pre>
 * Base64 keeps arbitrary keys/values (tabs, newlines, unicode) unambiguous
 * without pulling in a JSON library. A corrupt or missing file loads as an
 * empty cache instead of crashing startup; corrupt lines are skipped.
 */
public final class Persistence {

    private Persistence() {}

    public static void save(Path file, List<CacheEntry> entries) throws IOException {
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try (BufferedWriter w = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
            for (CacheEntry e : entries) {
                w.write(encode(e.key()));
                w.write('\t');
                w.write(encode(e.value()));
                w.write('\t');
                w.write(Long.toString(e.expiresAtMillis()));
                w.write('\t');
                w.write(Long.toString(e.createdAtMillis()));
                w.newLine();
            }
        }
        // Atomic-ish replace: a crash mid-write leaves the old snapshot intact.
        Files.move(tmp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    public static List<CacheEntry> load(Path file) throws IOException {
        List<CacheEntry> entries = new ArrayList<>();
        if (!Files.exists(file)) {
            return entries;
        }
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split("\t", -1);
                if (parts.length != 4) {
                    continue; // corrupt line: skip, don't fail startup
                }
                try {
                    entries.add(new CacheEntry(
                            decode(parts[0]),
                            decode(parts[1]),
                            Long.parseLong(parts[2]),
                            Long.parseLong(parts[3])));
                } catch (IllegalArgumentException ignored) {
                    // Bad Base64 or bad number: skip the line.
                }
            }
        }
        return entries;
    }

    private static String encode(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String s) {
        return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
    }
}
