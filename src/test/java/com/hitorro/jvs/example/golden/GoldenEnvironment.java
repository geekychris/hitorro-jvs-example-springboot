package com.hitorro.jvs.example.golden;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A fingerprint of the configuration and data a golden run uses, recorded as the first golden section.
 * JVS enrichment (segmentation, NER, stems) depends on HT_BIN's config and NLP data, so responses are only
 * comparable when this matches; the test fails with a plain message when it doesn't.
 */
final class GoldenEnvironment {

    /** Things in data/ the app or its tests rewrite, or that grow while it runs. */
    private static final Set<String> VOLATILE = Set.of("kvstore", "retrieval-kvstore", "git-metadata.json");

    private GoldenEnvironment() {}

    static String describe(String mode, Path htBin) {
        StringBuilder sb = new StringBuilder("mode: ").append(mode).append('\n');
        sb.append("HT_BIN config (file sha256):\n");
        files(htBin.resolve("config")).forEach(f -> sb.append("  ").append(rel(htBin, f)).append(' ').append(sha(f)).append('\n'));
        sb.append("app config/types (file sha256):\n");
        Path app = Path.of("").toAbsolutePath();
        files(app.resolve("config/types")).forEach(f -> sb.append("  ").append(rel(app, f)).append(' ').append(sha(f)).append('\n'));
        sb.append("HT_BIN data (entry: files, bytes):\n");
        try (Stream<Path> top = Files.list(htBin.resolve("data"))) {
            top.filter(p -> !VOLATILE.contains(name(p)) && !name(p).endsWith(".db") && !name(p).startsWith("demo-documents-")
                            && !name(p).startsWith("."))
                    .sorted()
                    .forEach(p -> {
                        List<Path> fs = files(p);
                        long bytes = fs.stream().mapToLong(GoldenEnvironment::size).sum();
                        sb.append("  ").append(name(p)).append(": ").append(fs.size()).append(", ").append(bytes).append('\n');
                    });
        } catch (IOException e) {
            sb.append("  <no data dir>\n");
        }
        return sb.toString().stripTrailing();
    }

    private static List<Path> files(Path dir) {
        if (!Files.exists(dir)) return List.of();
        if (Files.isRegularFile(dir)) return List.of(dir);
        try (Stream<Path> s = Files.walk(dir)) {
            return s.filter(Files::isRegularFile).filter(p -> !name(p).startsWith(".")).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String rel(Path base, Path f) {
        return base.relativize(f).toString().replace('\\', '/');
    }

    private static String name(Path p) {
        return p.getFileName().toString();
    }

    private static long size(Path p) {
        try {
            return Files.size(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String sha(Path f) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(f));
            return HexFormat.of().formatHex(d).substring(0, 12);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
