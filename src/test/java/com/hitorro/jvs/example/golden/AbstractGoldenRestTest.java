package com.hitorro.jvs.example.golden;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * Golden REST test: every controller's JSON output must be exactly what the app returned on Jackson 2
 * (Spring Boot 4.1.3 with the spring-boot-jackson2 bridge, main before the Jackson 3 migration).
 *
 * <p>{@code src/test/resources/golden/jackson2-rest.txt} was recorded by running this same class on main
 * with {@code -Dgolden.record=true}. Controllers covered: JvsController, NdjsonPipelineController
 * (responses and the NDJSON files it writes), SearchController, LuceneViewerController,
 * RetrievalController (including the NDJSON stream), WireTransportController (including the NDJSON
 * stream), FeatureController, GitToolsController (against a fixture repository with fixed dates),
 * UnitTimeController, plus a probe of the MVC JSON converter itself ({@link ConverterProbe}).
 *
 * <p>Ollama is pointed at a closed port so translation and summaries are deterministic; the remote
 * wire transport (HTTP to localhost:8080) is not switched on.
 */
@SpringBootTest(properties = "golden.rest=true") // own context: services read system properties at startup
@AutoConfigureMockMvc
@DisplayName("Golden REST responses (recorded on Jackson 2)")
abstract class AbstractGoldenRestTest {

    /** The HT_BIN (config + data) this run uses, and the golden file recorded with it; set by the subclass. */
    private static Path htBin;
    private static Path golden;
    private static String mode;
    private static final Path FIXTURE = Path.of("target/golden-git-fixture").toAbsolutePath();
    private static final List<String> WRITTEN_FILES = List.of(
            "demo-documents-enriched.ndjson", "demo-documents-enriched-stream.ndjson",
            "demo-documents-translated.ndjson", "demo-documents-translated-stream.ndjson",
            "demo-documents-combined.ndjson", "demo-documents-combined-stream.ndjson",
            "demo-documents-stream-to-sink.ndjson");
    private static final Path GIT_META = Path.of("data/git-metadata.json");
    private static final Map<Path, byte[]> saved = new LinkedHashMap<>();
    private static String previousOllama;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private RequestMappingHandlerAdapter adapter;

    @BeforeAll
    static void setUp() throws Exception {
        previousOllama = System.getProperty("ollama.url");
        System.setProperty("ollama.url", "http://127.0.0.1:9");
        // the app writes these files (some are committed): put them back afterwards
        for (String f : WRITTEN_FILES) save(Path.of("data", f));
        save(GIT_META);
        Files.deleteIfExists(GIT_META);
        createFixtureRepository();
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (previousOllama == null) System.clearProperty("ollama.url");
        else System.setProperty("ollama.url", previousOllama);
        for (var e : saved.entrySet()) {
            if (e.getValue() == null) Files.deleteIfExists(e.getKey());
            else Files.write(e.getKey(), e.getValue());
        }
    }

    private static void save(Path p) throws IOException {
        saved.put(p, Files.exists(p) ? Files.readAllBytes(p) : null);
    }

    /**
     * Fixes the environment before the Spring context starts: JvsService takes HT_BIN from this system
     * property (otherwise it guesses "..", then "."), and the type system and NLP read config and data there.
     */
    protected static void use(String runMode, Path htBinDir, String goldenFile) {
        mode = runMode;
        htBin = htBinDir.toAbsolutePath().normalize();
        golden = Path.of("src/test/resources/golden", goldenFile);
        System.setProperty("HT_BIN", htBin.toString());
        System.setProperty("HT_HOME", htBin.toString());
    }

    @Test
    @DisplayName("every endpoint returns the Jackson 2 JSON")
    void endpointsMatchJackson2() throws Exception {
        GoldenRecorder g = new GoldenRecorder(golden)
                .volatileKeys("timestamp|[A-Za-z]*TimeMs|timeMs|msPerUnit|nsPerUnit|instructionCycles|units|cpuGhz|durationMs")
                .replace(FIXTURE.toString(), "<FIXTURE>")
                .replace(htBin.toString(), "<HT_BIN>")
                .replace(Path.of("").toAbsolutePath().toString(), "<APP>")
                .replace(Path.of("").toAbsolutePath().getParent().toString(), "<APP_PARENT>")
                // Lucene lists its SPI names from a hash set: the order changes between runs
                .normalizer(s -> s.replaceAll("supports the following names: \\[[^\\]]*\\]", "supports the following names: [...]"))
                // Accepted difference: the parse error JVS.read reports is Jackson's own message, which names
                // the Jackson 3 exception class and location format ("byte offset" instead of "line, column").
                .acceptDifference("jvs propaccess bad json", "\"error\":\"[a-z.]+jackson[a-z.]+\\.[A-Za-z]+Exception: [^\"]*\"",
                        "\"error\":\"<Jackson parse error message>\"");

        // The environment comes first: a different config or data set changes enrichment, so say so plainly
        String env = GoldenEnvironment.describe(mode, htBin);
        if (!GoldenRecorder.RECORD) {
            String want = g.expected("environment");
            assertTrue(env.equals(want), "This run's HT_BIN config/data is not the one " + golden + " was recorded with,"
                    + " so the responses can't be compared.\nexpected:\n" + want + "\nactual:\n" + env);
        }
        g.put("environment", env);

        jvs(g);
        pipeline(g);
        search(g);
        retrieval(g);
        features(g);
        gittools(g);
        unittime(g);
        ConverterProbe.run(ConverterProbe.jsonConverter(adapter), g);

        List<String> diffs = g.finish();
        assertTrue(diffs.isEmpty(), diffs.size() + " responses differ from Jackson 2:\n" + String.join("\n", diffs));
    }

    // ------------------------------------------------------------------ JvsController

    private static final String ARTICLE = """
        {"type":"demo_article","id":{"domain":"news","did":"a-1"},
         "title":{"mls":[{"lang":"en","text":"Running dogs in Berlin"}]},
         "body":{"mls":[{"lang":"en","text":"The dogs were running happily through the parks of Berlin on Monday."}]},
         "count":3,"price":12.50,"ratio":1.10,"flag":true,"none":null,"list":[1,2.0,"x",{"k":[]}]}
        """;

    private void jvs(GoldenRecorder g) throws Exception {
        g.rest(mvc, "jvs types", get("/api/jvs/types"));
        g.rest(mvc, "jvs type core_sysobject", get("/api/jvs/types/core_sysobject"));
        g.rest(mvc, "jvs type demo_article", get("/api/jvs/types/demo_article"));
        g.rest(mvc, "jvs type missing", get("/api/jvs/types/nonexistent_type"));
        g.restUnordered(mvc, "jvs status", get("/api/jvs/status"));
        g.rest(mvc, "jvs translate status", get("/api/jvs/translate/status"));
        g.rest(mvc, "jvs propaccess title", json(post("/api/jvs/propaccess"), "{\"json\":" + ARTICLE + ",\"path\":\"title.mls\"}"));
        g.rest(mvc, "jvs propaccess string", json(post("/api/jvs/propaccess"),
                "{\"json\":" + quote(ARTICLE) + ",\"path\":\"id.did\"}"));
        g.rest(mvc, "jvs propaccess number", json(post("/api/jvs/propaccess"), "{\"json\":" + ARTICLE + ",\"path\":\"price\"}"));
        g.rest(mvc, "jvs propaccess missing", json(post("/api/jvs/propaccess"), "{\"json\":" + ARTICLE + ",\"path\":\"nope.x\"}"));
        g.rest(mvc, "jvs propaccess bad json", json(post("/api/jvs/propaccess"), "{\"json\":\"{not json\",\"path\":\"a\"}"));
        g.rest(mvc, "jvs documents typed", json(post("/api/jvs/documents"), ARTICLE));
        g.rest(mvc, "jvs documents untyped", json(post("/api/jvs/documents"),
                "{\"id\":{\"did\":\"test-001\",\"domain\":\"demo\"},\"title\":{\"mls\":[{\"lang\":\"en\",\"text\":\"Test\"}]}}"));
        g.rest(mvc, "jvs merge", json(post("/api/jvs/merge"),
                "{\"base\":{\"name\":\"original\",\"count\":3,\"flag\":true,\"nested\":{\"list\":[1,2,\"x\"],\"keep\":1.5}},"
                        + "\"overlay\":{\"shared\":\"new\",\"nested\":{\"list\":[9],\"add\":null}}}"));
        g.rest(mvc, "jvs validate untyped", json(post("/api/jvs/validate"), "{\"title\":{\"mls\":[{\"lang\":\"en\",\"text\":\"x\"}]}}"));
        g.restUnordered(mvc, "jvs validate typed", json(post("/api/jvs/validate"), ARTICLE));
        g.restUnordered(mvc, "jvs validate invalid", json(post("/api/jvs/validate"),
                "{\"type\":\"demo_article\",\"id\":7,\"title\":\"not mls\",\"count\":\"three\"}"));
        g.rest(mvc, "jvs enrich untyped", json(post("/api/jvs/enrich"), "{\"title\":{\"mls\":[{\"lang\":\"en\",\"text\":\"NLP\"}]}}"));
        g.rest(mvc, "jvs enrich typed basic", json(post("/api/jvs/enrich"), "{\"json\":" + ARTICLE + ",\"tags\":\"basic\"}"));
        g.rest(mvc, "jvs enrich typed segmented", json(post("/api/jvs/enrich"),
                "{\"json\":" + quote(ARTICLE) + ",\"tags\":\"basic,segmented\"}"));
        g.restUnordered(mvc, "jvs stem en", json(post("/api/jvs/stem"), "{\"text\":\"The dogs were running happily\",\"language\":\"en\"}"));
        g.restUnordered(mvc, "jvs stem de", json(post("/api/jvs/stem"), "{\"text\":\"Die Hunde liefen glücklich\",\"language\":\"de\"}"));
        g.rest(mvc, "jvs translate", json(post("/api/jvs/translate"),
                "{\"json\":" + ARTICLE + ",\"mlsFields\":[\"title\"],\"sourceLanguage\":\"en\",\"targetLanguages\":[\"de\"]}"));
        // nesting: request bodies and responses up to Jackson 2's 1000 levels (Jackson 3's default is 500)
        g.rest(mvc, "jvs merge deep 600", json(post("/api/jvs/merge"),
                "{\"base\":" + deep(600) + ",\"overlay\":{\"o\":1}}"));
        g.rest(mvc, "jvs documents deep 998", json(post("/api/jvs/documents"), deep(998)));
        g.rest(mvc, "jvs documents deep 1001", json(post("/api/jvs/documents"), deep(1001)));
        g.rest(mvc, "jvs validate deep 700", json(post("/api/jvs/validate"), deep(700)));
        g.rest(mvc, "jvs malformed body", json(post("/api/jvs/merge"), "{\"base\":"));
        g.rest(mvc, "jvs wrong content type", post("/api/jvs/merge").contentType(MediaType.TEXT_PLAIN).content("x"));
    }

    // ------------------------------------------------------------------ NdjsonPipelineController

    private void pipeline(GoldenRecorder g) throws Exception {
        g.rest(mvc, "pipeline dataset", get("/api/jvs/pipeline/dataset"));
        g.restUnordered(mvc, "pipeline dataset count", get("/api/jvs/pipeline/dataset/count"));
        String[][] steps = {
                {"enrich/iterator", "{\"tags\":\"basic\"}"},
                {"enrich/stream", "{\"tags\":\"basic\"}"},
                {"translate/iterator", "{\"sourceLanguage\":\"en\",\"targetLanguages\":[\"de\"]}"},
                {"translate/stream", "{\"sourceLanguage\":\"en\",\"targetLanguages\":[\"de\"]}"},
                {"combined/iterator", "{\"tags\":\"basic\",\"targetLanguages\":[\"es\"]}"},
                {"combined/stream", "{\"tags\":\"basic\",\"targetLanguages\":[\"es\"]}"},
                {"bridge/iterator-to-stream", "{\"tags\":\"basic\"}"},
                {"bridge/stream-to-sink", "{\"tags\":\"basic\"}"},
        };
        for (String[] s : steps) {
            g.rest(mvc, "pipeline " + s[0], json(post("/api/jvs/pipeline/" + s[0]), s[1]));
        }
        g.rest(mvc, "pipeline enrich/iterator no body", post("/api/jvs/pipeline/enrich/iterator"));
        // the NDJSON files the pipelines wrote (stored JSON)
        for (String f : WRITTEN_FILES) {
            Path p = Path.of("data", f);
            g.put("pipeline file " + f, Files.exists(p) ? Files.readString(p, StandardCharsets.UTF_8) : "<absent>");
        }
    }

    // ------------------------------------------------------------------ SearchController + LuceneViewerController

    private void search(GoldenRecorder g) throws Exception {
        g.rest(mvc, "search status before", get("/api/jvs/search/status"));
        g.rest(mvc, "viewer stats before index", get("/api/jvs/viewer/stats"));
        g.rest(mvc, "search index", json(post("/api/jvs/search/index"), "{\"enrichTags\":\"basic\"}"));
        g.rest(mvc, "search status", get("/api/jvs/search/status"));
        g.rest(mvc, "search all", get("/api/jvs/search").param("q", "*:*").param("limit", "3"));
        g.rest(mvc, "search facets", get("/api/jvs/search").param("q", "*:*").param("facets", "type").param("useKvStore", "false"));
        g.rest(mvc, "search term", get("/api/jvs/search").param("q", "merger").param("useKvStore", "false"));
        g.rest(mvc, "search bad query", get("/api/jvs/search").param("q", "title:(unclosed"));
        g.rest(mvc, "search doc", get("/api/jvs/search/doc/legal/doc-001"));
        g.rest(mvc, "search doc missing", get("/api/jvs/search/doc/legal/none"));
        g.rest(mvc, "search fields", get("/api/jvs/search/fields/demo_document"));
        g.rest(mvc, "viewer stats", get("/api/jvs/viewer/stats"));
        g.rest(mvc, "viewer fields", get("/api/jvs/viewer/fields"));
        g.rest(mvc, "viewer documents", get("/api/jvs/viewer/documents").param("limit", "2"));
        g.rest(mvc, "viewer document 0", get("/api/jvs/viewer/documents/0"));
        g.rest(mvc, "viewer document out of range", get("/api/jvs/viewer/documents/9999"));
        g.rest(mvc, "viewer terms id", get("/api/jvs/viewer/terms/id.did").param("limit", "5"));
        g.rest(mvc, "viewer search", get("/api/jvs/viewer/search").param("q", "merger").param("limit", "2"));
    }

    // ------------------------------------------------------------------ RetrievalController + WireTransportController

    private void retrieval(GoldenRecorder g) throws Exception {
        g.rest(mvc, "retrieval status empty", get("/api/retrieval/status"));
        g.rest(mvc, "retrieval datasets", get("/api/retrieval/datasets"));
        g.rest(mvc, "retrieval create index", json(post("/api/retrieval/indexes"), "{\"name\":\"golden\",\"typeName\":\"demo_document\"}"));
        g.rest(mvc, "retrieval recreate index", json(post("/api/retrieval/indexes"), "{\"name\":\"golden\",\"typeName\":\"demo_document\"}"));
        g.rest(mvc, "retrieval index documents dataset", json(post("/api/retrieval/indexes/golden/documents"), "{\"source\":\"dataset\"}"));
        g.rest(mvc, "retrieval index documents array", json(post("/api/retrieval/indexes/golden/documents"),
                "[{\"type\":\"demo_document\",\"id\":{\"domain\":\"g\",\"did\":\"g-1\"},\"title\":{\"mls\":[{\"lang\":\"en\",\"text\":\"Golden one\"}]}}]"));
        g.rest(mvc, "retrieval index documents missing index", json(post("/api/retrieval/indexes/nope/documents"), "{}"));
        g.rest(mvc, "retrieval kvstore enable", post("/api/retrieval/kvstore/enable"));
        g.rest(mvc, "retrieval load articles", json(post("/api/retrieval/datasets/load"), "{\"dataset\":\"ARTICLES\"}"));
        g.rest(mvc, "retrieval load products enriched", json(post("/api/retrieval/datasets/load"), "{\"dataset\":\"PRODUCTS\",\"enrichTags\":\"basic\"}"));
        g.rest(mvc, "retrieval load demo", json(post("/api/retrieval/datasets/load"), "{\"dataset\":\"DEMO_DOCUMENTS\"}"));
        g.rest(mvc, "retrieval load unknown", json(post("/api/retrieval/datasets/load"), "{\"dataset\":\"NOPE\"}"));
        g.rest(mvc, "retrieval status", get("/api/retrieval/status"));
        g.rest(mvc, "retrieval indexes", get("/api/retrieval/indexes"));
        g.rest(mvc, "retrieval fields demo", get("/api/retrieval/fields/demo_documents"));
        g.rest(mvc, "retrieval fields articles", get("/api/retrieval/fields/articles"));
        g.rest(mvc, "retrieval search-multiple", json(post("/api/retrieval/search-multiple"),
                "{\"indexNames\":[\"articles\",\"products\"],\"query\":\"*:*\",\"limit\":4,\"merger\":\"round_robin\"}"));
        g.rest(mvc, "retrieval search-multiple kv facets", json(post("/api/retrieval/search-multiple"),
                "{\"indexNames\":[\"articles\"],\"query\":\"*:*\",\"limit\":2,\"useKvStore\":true,\"facets\":\"category\"}"));
        g.rest(mvc, "retrieval search-multiple none", json(post("/api/retrieval/search-multiple"), "{\"indexNames\":[\"nope\"]}"));
        String q = "{\"search\":{\"query\":\"*:*\",\"offset\":0,\"limit\":5},\"fetch\":{\"enabled\":true},"
                + "\"page\":{\"rows\":2,\"page\":0}}";
        g.rest(mvc, "retrieval execute", json(post("/api/retrieval/execute"), "{\"indexName\":\"articles\",\"query\":" + q + ",\"lang\":\"en\"}"));
        g.rest(mvc, "retrieval execute facets fixup", json(post("/api/retrieval/execute"),
                "{\"indexName\":\"demo_documents\",\"query\":{\"search\":{\"query\":\"*:*\",\"limit\":3,\"facets\":[\"type\"]},"
                        + "\"fixup\":{\"tags\":[\"basic\"]}}}"));
        g.rest(mvc, "retrieval execute summarize", json(post("/api/retrieval/execute"),
                "{\"indexName\":\"articles\",\"query\":{\"search\":{\"query\":\"*:*\",\"limit\":2},"
                        + "\"summarize\":{\"enabled\":true,\"maxDocs\":2,\"maxWords\":20}}}"));
        g.rest(mvc, "retrieval execute missing index", json(post("/api/retrieval/execute"), "{\"indexName\":\"nope\",\"query\":{}}"));
        g.rest(mvc, "retrieval execute stream", json(post("/api/retrieval/execute/stream"),
                "{\"indexName\":\"articles\",\"query\":" + q + "}"));
        g.rest(mvc, "retrieval viewer stats", get("/api/retrieval/viewer/articles/stats"));
        g.rest(mvc, "retrieval viewer fields", get("/api/retrieval/viewer/articles/fields"));
        g.rest(mvc, "retrieval viewer documents", get("/api/retrieval/viewer/articles/documents").param("limit", "2"));
        g.rest(mvc, "retrieval viewer terms", get("/api/retrieval/viewer/articles/terms/type").param("limit", "5"));
        g.rest(mvc, "retrieval viewer search", get("/api/retrieval/viewer/articles/search").param("q", "*:*").param("limit", "2"));
        g.rest(mvc, "retrieval viewer postings", get("/api/retrieval/viewer/articles/postings/type"));
        g.rest(mvc, "retrieval viewer missing", get("/api/retrieval/viewer/nope/stats"));
        g.rest(mvc, "retrieval analyzer tokenizers", get("/api/retrieval/analyzer/tokenizers"));
        g.rest(mvc, "retrieval analyzer filters", get("/api/retrieval/analyzer/filters"));
        g.rest(mvc, "retrieval analyze", json(post("/api/retrieval/analyzer/analyze"),
                "{\"tokenizer\":\"standard\",\"filters\":[\"lowercase\",\"porterstem\"],\"text\":\"The Running Dogs, 2024!\"}"));
        g.rest(mvc, "retrieval analyze bad tokenizer", json(post("/api/retrieval/analyzer/analyze"), "{\"tokenizer\":\"nope\",\"text\":\"x\"}"));
        g.rest(mvc, "retrieval remote search", get("/api/retrieval/search").param("index", "articles").param("q", "*:*").param("limit", "2"));
        g.rest(mvc, "retrieval remote search facets", get("/api/retrieval/search").param("index", "articles").param("q", "*:*")
                .param("limit", "1").param("facets", "category"));
        g.rest(mvc, "retrieval remote document", get("/api/retrieval/documents/articles/art-001"));
        g.rest(mvc, "retrieval remote document missing", get("/api/retrieval/documents/none"));
        g.restUnordered(mvc, "retrieval health", get("/api/retrieval/health"));
        g.rest(mvc, "retrieval transport status", get("/api/retrieval/transport/status"));
        g.restUnordered(mvc, "retrieval transport toggle on", post("/api/retrieval/transport/toggle"));
        g.restUnordered(mvc, "retrieval transport toggle off", post("/api/retrieval/transport/toggle"));
        g.rest(mvc, "wire search", get("/api/search").param("index", "articles").param("q", "*:*").param("limit", "2"));
        g.rest(mvc, "wire search stream", get("/api/search/stream").param("index", "articles").param("q", "*:*").param("limit", "2"));
        g.rest(mvc, "wire search stream facets", get("/api/search/stream").param("index", "articles").param("q", "*:*")
                .param("limit", "1").param("facets", "category"));
        g.rest(mvc, "wire document", get("/api/documents/articles%2Fart-001"));
        g.restUnordered(mvc, "wire health", get("/api/health"));
        g.rest(mvc, "retrieval delete index", delete("/api/retrieval/indexes/golden"));
        g.rest(mvc, "retrieval delete index missing", delete("/api/retrieval/indexes/golden"));
        g.rest(mvc, "retrieval kvstore disable", post("/api/retrieval/kvstore/disable"));
    }

    // ------------------------------------------------------------------ FeatureController

    private void features(GoldenRecorder g) throws Exception {
        g.rest(mvc, "features status", get("/api/features/status"));
        g.restUnordered(mvc, "features index empty", post("/api/features/index"));
        g.restUnordered(mvc, "features query before index", get("/api/features/query").param("docId", "1").param("feature", "depth"));
        g.rest(mvc, "features extract", json(post("/api/features/extract"),
                "{\"id\":1,\"title\":\"Hello world\",\"body\":\"one two three four\",\"date\":\"2026-01-01\",\"tags\":[\"a\"],\"n\":{\"m\":{\"o\":1}}}"));
        g.rest(mvc, "features extract batch", json(post("/api/features/extract-batch"),
                "[{\"id\":2,\"text\":\"alpha beta\"},{\"content\":\"x y z\",\"nested\":[[1]]},{\"title\":7}]"));
        g.rest(mvc, "features index", post("/api/features/index"));
        g.rest(mvc, "features query", get("/api/features/query").param("docId", "1").param("feature", "word_count"));
        g.restUnordered(mvc, "features query unknown", get("/api/features/query").param("docId", "1").param("feature", "nope"));
        g.rest(mvc, "features extracted", get("/api/features/extracted"));
        g.restUnordered(mvc, "features clear", post("/api/features/clear"));
        g.rest(mvc, "features status after clear", get("/api/features/status"));
    }

    // ------------------------------------------------------------------ GitToolsController

    private void gittools(GoldenRecorder g) throws Exception {
        g.restUnordered(mvc, "git status empty", get("/api/gittools/status"));
        g.rest(mvc, "git scan", json(post("/api/gittools/scan"), "{\"roots\":[\"" + FIXTURE + "\"],\"maxDepth\":2}"));
        g.rest(mvc, "git repos", get("/api/gittools/repos"));
        g.rest(mvc, "git repos by name", get("/api/gittools/repos").param("sort", "name"));
        g.rest(mvc, "git repos by date", get("/api/gittools/repos").param("sort", "date"));
        g.rest(mvc, "git branches", get("/api/gittools/repos/0/branches"));
        g.rest(mvc, "git tags", get("/api/gittools/repos/0/tags"));
        String commits = g.rest(mvc, "git commits", get("/api/gittools/repos/0/commits"));
        g.rest(mvc, "git commits filtered", get("/api/gittools/repos/0/commits").param("maxCount", "1").param("author", "Golden"));
        Matcher m = Pattern.compile("[0-9a-f]{40}").matcher(commits);
        String hash = m.find() ? m.group() : "0000000000000000000000000000000000000000";
        g.restUnordered(mvc, "git commit detail", get("/api/gittools/repos/0/commits/" + hash));
        g.restUnordered(mvc, "git commit diff", get("/api/gittools/repos/0/commits/" + hash + "/diff"));
        g.rest(mvc, "git commit missing", get("/api/gittools/repos/0/commits/ffffffffffffffffffffffffffffffffffffffff"));
        g.restUnordered(mvc, "git add tag", json(post("/api/gittools/repos/0/tag"), "{\"tag\":\"golden\"}"));
        g.restUnordered(mvc, "git add tag 2", json(post("/api/gittools/repos/0/tag"), "{\"tag\":\"alpha\"}"));
        g.rest(mvc, "git all tags", get("/api/gittools/tags"));
        g.rest(mvc, "git repos by tag", get("/api/gittools/repos").param("tag", "golden"));
        g.restUnordered(mvc, "git remove tag", delete("/api/gittools/repos/0/tag/alpha"));
        g.restUnordered(mvc, "git description", json(post("/api/gittools/repos/0/description"), "{\"description\":\"Golden fixture\"}"));
        g.rest(mvc, "git repos after edits", get("/api/gittools/repos"));
        g.restUnordered(mvc, "git status", get("/api/gittools/status"));
        g.rest(mvc, "git repo out of range", get("/api/gittools/repos/99/branches"));
        g.put("git metadata file", Files.exists(GIT_META) ? Files.readString(GIT_META, StandardCharsets.UTF_8) : "<absent>");
    }

    // ------------------------------------------------------------------ UnitTimeController

    private void unittime(GoldenRecorder g) throws Exception {
        g.rest(mvc, "unittime status", get("/api/unittime/status"));
        String cats = g.rest(mvc, "unittime categories", get("/api/unittime/categories"));
        Matcher m = Pattern.compile("\"([^\"]+)\"").matcher(cats);
        String first = m.find() ? m.group(1) : "none";
        g.rest(mvc, "unittime run filtered", get("/api/unittime/run").param("filter", first).param("ghz", "4.0"));
    }

    // ------------------------------------------------------------------ helpers

    private static MockHttpServletRequestBuilder json(MockHttpServletRequestBuilder b, String body) {
        return b.contentType(MediaType.APPLICATION_JSON).content(body);
    }

    /** {@code depth} nested objects (the outermost included). */
    private static String deep(int depth) {
        return "{\"a\":".repeat(depth - 1) + "{\"leaf\":1}" + "}".repeat(depth - 1);
    }

    private static String quote(String json) {
        return "\"" + json.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }

    /** A git repository with fixed author, dates and content, so hashes and dates are the same every run. */
    private static void createFixtureRepository() throws Exception {
        Path repo = FIXTURE.resolve("golden-repo");
        if (Files.exists(FIXTURE)) {
            try (var s = Files.walk(FIXTURE)) {
                s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
        Files.createDirectories(repo);
        git(repo, null, "init", "-q", "-b", "main");
        Files.writeString(repo.resolve("README.md"), "# Golden\n");
        git(repo, null, "add", "README.md");
        git(repo, "2024-03-15T10:00:00Z", "commit", "-q", "-m", "First commit");
        Files.writeString(repo.resolve("README.md"), "# Golden\n\nSecond line\n");
        Files.writeString(repo.resolve("data.json"), "{\"a\":1}\n");
        git(repo, null, "add", "README.md", "data.json");
        git(repo, "2024-04-01T08:30:00Z", "commit", "-q", "-m", "Second commit\n\nWith a body");
        git(repo, "2024-04-01T08:30:00Z", "tag", "-a", "v1.0", "-m", "Release 1.0");
        git(repo, null, "branch", "feature/x");
    }

    private static void git(Path dir, String date, String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("git");
        cmd.add("-c");
        cmd.add("commit.gpgsign=false");
        cmd.add("-c");
        cmd.add("tag.gpgsign=false");
        cmd.addAll(List.of(args));
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(dir.toFile()).redirectErrorStream(true);
        Map<String, String> env = pb.environment();
        env.put("GIT_AUTHOR_NAME", "Golden Tester");
        env.put("GIT_AUTHOR_EMAIL", "golden@example.com");
        env.put("GIT_COMMITTER_NAME", "Golden Tester");
        env.put("GIT_COMMITTER_EMAIL", "golden@example.com");
        env.put("GIT_CONFIG_NOSYSTEM", "1");
        env.put("GIT_CONFIG_GLOBAL", "/dev/null");
        if (date != null) {
            env.put("GIT_AUTHOR_DATE", date);
            env.put("GIT_COMMITTER_DATE", date);
        }
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) throw new IllegalStateException("git " + args[0] + " failed: " + out);
    }
}
