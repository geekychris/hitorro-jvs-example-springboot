package com.hitorro.jvs.example.golden;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import java.nio.file.Path;

/**
 * Golden REST test with the app's own config and data as HT_BIN (the checkout itself), so it gives the same
 * result in any checkout and layout, whatever {@code ../config} or {@code ../data} hold. In this environment
 * the NLP data is absent, so JVS enrichment adds nothing; {@link GoldenRestFullTest} covers enrichment.
 * Recorded on main: {@code src/test/resources/golden/jackson2-rest.txt}.
 */
@DisplayName("Golden REST responses, app config (recorded on Jackson 2)")
class GoldenRestTest extends AbstractGoldenRestTest {

    @BeforeAll
    static void appConfig() {
        use("app", Path.of(""), "jackson2-rest.txt");
    }
}
