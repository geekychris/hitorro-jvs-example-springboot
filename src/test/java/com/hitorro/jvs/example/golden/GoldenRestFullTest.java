package com.hitorro.jvs.example.golden;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.test.context.TestPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Golden REST test with a full HiTorro runtime as HT_BIN (hitorro-all's {@code config} and {@code data},
 * including the NLP models), so JVS enrichment really runs and its output is pinned too. Runs only when it
 * is told where that runtime is: {@code -Dgolden.htbin=/path/to/hitorro-all}. It never guesses from
 * {@code ../}. Recorded on main with hitorro-all at commit 96e9dcd:
 * {@code src/test/resources/golden/jackson2-rest-full.txt}; the environment section pins that config and data.
 */
@EnabledIfSystemProperty(named = "golden.htbin", matches = ".+")
@TestPropertySource(properties = "golden.mode=full") // a context of its own
@DisplayName("Golden REST responses, full HiTorro runtime (recorded on Jackson 2)")
class GoldenRestFullTest extends AbstractGoldenRestTest {

    @BeforeAll
    static void fullRuntime() {
        Path root = Path.of(System.getProperty("golden.htbin"));
        assertTrue(Files.isDirectory(root.resolve("config/jsonconfigs")) && Files.isDirectory(root.resolve("data/opennlpmodels1.5")),
                "-Dgolden.htbin must point at a full HiTorro runtime (config/jsonconfigs and data/opennlpmodels1.5): " + root);
        use("full", root, "jackson2-rest-full.txt");
    }
}
