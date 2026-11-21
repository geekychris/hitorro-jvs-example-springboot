package com.hitorro.jvs.example.golden;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.test.context.TestPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Golden REST test with a full HiTorro runtime as HT_BIN (hitorro-all's {@code config} and {@code data},
 * including the NLP models), so JVS enrichment really runs and its output is pinned too. It runs only when
 * told where that runtime is, with {@code -Dhitorro.all=/path/to/hitorro-all} or the {@code HITORRO_ALL}
 * environment variable, and is skipped otherwise; it never guesses from {@code ../}. Point it at a copy:
 * the app writes into HT_BIN's data. Recorded on main with hitorro-all at commit 96e9dcd:
 * {@code src/test/resources/golden/jackson2-rest-full.txt}; its environment section pins that config and data.
 */
@EnabledIf(value = "configured", disabledReason = "the enrichment golden needs a full hitorro-all runtime:"
        + " pass -Dhitorro.all=/path/to/hitorro-all or set HITORRO_ALL (a copy, not a working install)")
@TestPropertySource(properties = "golden.mode=full") // a context of its own
@DisplayName("Golden REST responses, full HiTorro runtime (recorded on Jackson 2)")
class GoldenRestFullTest extends AbstractGoldenRestTest {

    static String dir() {
        return System.getProperty("hitorro.all", System.getenv("HITORRO_ALL"));
    }

    static boolean configured() {
        return dir() != null && !dir().isBlank();
    }

    @BeforeAll
    static void fullRuntime() {
        Path root = Path.of(dir());
        assertTrue(Files.isDirectory(root.resolve("config/jsonconfigs")) && Files.isDirectory(root.resolve("data/opennlpmodels1.5")),
                "hitorro.all / HITORRO_ALL must point at a full HiTorro runtime (config/jsonconfigs and data/opennlpmodels1.5): " + root);
        use("full", root, "jackson2-rest-full.txt");
    }
}
