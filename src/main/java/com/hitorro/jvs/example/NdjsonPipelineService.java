package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.JVS2JVSTranslationMapper;
import com.hitorro.util.core.iterator.AbstractIterator;
import com.hitorro.util.core.iterator.JSONIterator;
import com.hitorro.util.core.iterator.sinks.JsonSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class NdjsonPipelineService {

    private static final Logger log = LoggerFactory.getLogger(NdjsonPipelineService.class);

    private final JvsService jvsService;

    public NdjsonPipelineService(JvsService jvsService) {
        this.jvsService = jvsService;
    }

    // ─── Dataset Access ───────────────────────────────────────────

    public Path getDatasetPath() {
        for (String base : new String[]{".", ".."}) {
            Path p = Path.of(base, "data", "demo-documents.ndjson");
            if (Files.exists(p)) return p;
        }
        return Path.of("data", "demo-documents.ndjson");
    }

    public List<JsonNode> readDataset() throws Exception {
        Path path = getDatasetPath();
        List<JsonNode> docs = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
             JSONIterator iter = new JSONIterator(reader)) {
            while (iter.hasNext()) {
                docs.add(iter.next());
            }
        }
        return docs;
    }

    // ─── Hitorro Iterator Pipeline: Enrich with NER ───────────────

    public Map<String, Object> enrichPipelineIterator(String tags) throws Exception {
        long start = System.currentTimeMillis();
        String[] tagArray = (tags != null && !tags.isEmpty())
                ? tags.split(",") : new String[]{"basic", "segmented", "ner"};

        Path inputPath = getDatasetPath();
        Path outputPath = inputPath.resolveSibling("demo-documents-enriched.ndjson");

        JvsEnrichMapper enrichMapper = new JvsEnrichMapper(tagArray);
        List<JsonNode> results = new ArrayList<>();
        int count = 0;

        try (Reader reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8);
             JSONIterator jsonIter = new JSONIterator(reader);
             OutputStream os = Files.newOutputStream(outputPath)) {

            JsonSink sink = new JsonSink(os);
            sink.start();

            // Iterator pipeline: read -> convert to JVS -> enrich -> write
            AbstractIterator<JsonNode> pipeline = jsonIter
                    .map(json -> {
                        JVS jvs = new JVS(json);
                        return enrichMapper.enrich(jvs).getJsonNode();
                    });

            while (pipeline.hasNext()) {
                JsonNode enriched = pipeline.next();
                sink.add(enriched);
                results.add(enriched);
                count++;
            }
            sink.stop();
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", "iterator");
        result.put("pipeline", "enrich");
        result.put("tags", tagArray);
        result.put("inputFile", inputPath.toString());
        result.put("outputFile", outputPath.toString());
        result.put("documentCount", count);
        result.put("timeMs", System.currentTimeMillis() - start);
        result.put("documents", results);
        return result;
    }

    // ─── Java Streams Pipeline: Enrich with NER ───────────────────

    public Map<String, Object> enrichPipelineStream(String tags) throws Exception {
        long start = System.currentTimeMillis();
        String[] tagArray = (tags != null && !tags.isEmpty())
                ? tags.split(",") : new String[]{"basic", "segmented", "ner"};

        Path inputPath = getDatasetPath();
        Path outputPath = inputPath.resolveSibling("demo-documents-enriched-stream.ndjson");

        JvsEnrichMapper enrichMapper = new JvsEnrichMapper(tagArray);

        // Read all docs first so we can use a proper stream pipeline
        List<JsonNode> inputDocs = readDataset();

        // Stream pipeline: stream -> map to JVS -> enrich -> collect
        List<JsonNode> results = inputDocs.stream()
                .map(json -> new JVS(json))
                .map(jvs -> enrichMapper.enrich(jvs))
                .map(JVS::getJsonNode)
                .collect(Collectors.toList());

        // Write results to file
        writeNdjsonFile(outputPath, results);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", "stream");
        result.put("pipeline", "enrich");
        result.put("tags", tagArray);
        result.put("inputFile", inputPath.toString());
        result.put("outputFile", outputPath.toString());
        result.put("documentCount", results.size());
        result.put("timeMs", System.currentTimeMillis() - start);
        result.put("documents", results);
        return result;
    }

    // ─── Hitorro Iterator Pipeline: Translate ─────────────────────

    public Map<String, Object> translatePipelineIterator(String sourceLang,
                                                          List<String> targetLangs) throws Exception {
        long start = System.currentTimeMillis();
        if (sourceLang == null) sourceLang = "en";
        if (targetLangs == null || targetLangs.isEmpty()) targetLangs = List.of("de", "es");

        Path inputPath = getDatasetPath();
        Path outputPath = inputPath.resolveSibling("demo-documents-translated.ndjson");

        JVS2JVSTranslationMapper translationMapper = buildTranslationMapper(sourceLang, targetLangs);
        List<JsonNode> results = new ArrayList<>();
        int count = 0;

        try (Reader reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8);
             JSONIterator jsonIter = new JSONIterator(reader);
             OutputStream os = Files.newOutputStream(outputPath)) {

            JsonSink sink = new JsonSink(os);
            sink.start();

            // Iterator pipeline: read -> translate via mapper -> write
            AbstractIterator<JsonNode> pipeline = jsonIter
                    .map(json -> translationMapper.apply(new JVS(json)).getJsonNode());

            while (pipeline.hasNext()) {
                JsonNode translated = pipeline.next();
                sink.add(translated);
                results.add(translated);
                count++;
            }
            sink.stop();
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", "iterator");
        result.put("pipeline", "translate");
        result.put("sourceLanguage", sourceLang);
        result.put("targetLanguages", targetLangs);
        result.put("inputFile", inputPath.toString());
        result.put("outputFile", outputPath.toString());
        result.put("documentCount", count);
        result.put("timeMs", System.currentTimeMillis() - start);
        result.put("documents", results);
        return result;
    }

    // ─── Java Streams Pipeline: Translate ─────────────────────────

    public Map<String, Object> translatePipelineStream(String sourceLang,
                                                        List<String> targetLangs) throws Exception {
        long start = System.currentTimeMillis();
        if (sourceLang == null) sourceLang = "en";
        if (targetLangs == null || targetLangs.isEmpty()) targetLangs = List.of("de", "es");

        Path inputPath = getDatasetPath();
        Path outputPath = inputPath.resolveSibling("demo-documents-translated-stream.ndjson");

        JVS2JVSTranslationMapper translationMapper = buildTranslationMapper(sourceLang, targetLangs);
        List<JsonNode> inputDocs = readDataset();

        // Stream pipeline: stream -> translate via mapper -> collect
        List<JsonNode> results = inputDocs.stream()
                .map(json -> translationMapper.apply(new JVS(json)).getJsonNode())
                .collect(Collectors.toList());

        writeNdjsonFile(outputPath, results);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", "stream");
        result.put("pipeline", "translate");
        result.put("sourceLanguage", sourceLang);
        result.put("targetLanguages", targetLangs);
        result.put("inputFile", inputPath.toString());
        result.put("outputFile", outputPath.toString());
        result.put("documentCount", results.size());
        result.put("timeMs", System.currentTimeMillis() - start);
        result.put("documents", results);
        return result;
    }

    // ─── Hitorro Iterator Pipeline: Combined Enrich + Translate ───

    public Map<String, Object> combinedPipelineIterator(String tags, String sourceLang,
                                                         List<String> targetLangs) throws Exception {
        long start = System.currentTimeMillis();
        String[] tagArray = (tags != null && !tags.isEmpty())
                ? tags.split(",") : new String[]{"basic", "segmented", "ner"};
        if (sourceLang == null) sourceLang = "en";
        if (targetLangs == null || targetLangs.isEmpty()) targetLangs = List.of("de", "es");

        Path inputPath = getDatasetPath();
        Path outputPath = inputPath.resolveSibling("demo-documents-combined.ndjson");

        JvsEnrichMapper enrichMapper = new JvsEnrichMapper(tagArray);
        JVS2JVSTranslationMapper translationMapper = buildTranslationMapper(sourceLang, targetLangs);
        List<JsonNode> results = new ArrayList<>();
        int count = 0;

        try (Reader reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8);
             JSONIterator jsonIter = new JSONIterator(reader);
             OutputStream os = Files.newOutputStream(outputPath)) {

            JsonSink sink = new JsonSink(os);
            sink.start();

            // Combined pipeline: read -> enrich -> translate -> write
            AbstractIterator<JsonNode> pipeline = jsonIter
                    .map(json -> {
                        JVS jvs = new JVS(json);
                        JVS enriched = enrichMapper.enrich(jvs);
                        return translationMapper.apply(enriched).getJsonNode();
                    });

            while (pipeline.hasNext()) {
                JsonNode doc = pipeline.next();
                sink.add(doc);
                results.add(doc);
                count++;
            }
            sink.stop();
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", "iterator");
        result.put("pipeline", "combined (enrich + translate)");
        result.put("tags", tagArray);
        result.put("sourceLanguage", sourceLang);
        result.put("targetLanguages", targetLangs);
        result.put("inputFile", inputPath.toString());
        result.put("outputFile", outputPath.toString());
        result.put("documentCount", count);
        result.put("timeMs", System.currentTimeMillis() - start);
        result.put("documents", results);
        return result;
    }

    // ─── Java Streams Pipeline: Combined Enrich + Translate ───────

    public Map<String, Object> combinedPipelineStream(String tags, String sourceLang,
                                                       List<String> targetLangs) throws Exception {
        long start = System.currentTimeMillis();
        String[] tagArray = (tags != null && !tags.isEmpty())
                ? tags.split(",") : new String[]{"basic", "segmented", "ner"};
        if (sourceLang == null) sourceLang = "en";
        if (targetLangs == null || targetLangs.isEmpty()) targetLangs = List.of("de", "es");

        Path inputPath = getDatasetPath();
        Path outputPath = inputPath.resolveSibling("demo-documents-combined-stream.ndjson");

        JvsEnrichMapper enrichMapper = new JvsEnrichMapper(tagArray);
        JVS2JVSTranslationMapper translationMapper = buildTranslationMapper(sourceLang, targetLangs);
        List<JsonNode> inputDocs = readDataset();

        // Stream pipeline: stream -> enrich -> translate -> collect
        List<JsonNode> results = inputDocs.stream()
                .map(json -> {
                    JVS jvs = new JVS(json);
                    JVS enriched = enrichMapper.enrich(jvs);
                    return translationMapper.apply(enriched).getJsonNode();
                })
                .collect(Collectors.toList());

        writeNdjsonFile(outputPath, results);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", "stream");
        result.put("pipeline", "combined (enrich + translate)");
        result.put("tags", tagArray);
        result.put("sourceLanguage", sourceLang);
        result.put("targetLanguages", targetLangs);
        result.put("inputFile", inputPath.toString());
        result.put("outputFile", outputPath.toString());
        result.put("documentCount", results.size());
        result.put("timeMs", System.currentTimeMillis() - start);
        result.put("documents", results);
        return result;
    }

    // ─── Hitorro Iterator + toStream() bridge example ─────────────

    public Map<String, Object> iteratorToStreamExample(String tags) throws Exception {
        long start = System.currentTimeMillis();
        String[] tagArray = (tags != null && !tags.isEmpty())
                ? tags.split(",") : new String[]{"basic", "segmented", "ner"};

        Path inputPath = getDatasetPath();
        JvsEnrichMapper enrichMapper = new JvsEnrichMapper(tagArray);

        // Demonstrate hitorro iterator -> Java stream bridge
        List<JsonNode> results;
        try (Reader reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8);
             JSONIterator jsonIter = new JSONIterator(reader)) {

            // Use AbstractIterator.toStream() to bridge into Java Streams
            try (Stream<JsonNode> stream = jsonIter.toStream()) {
                results = stream
                        .map(json -> new JVS(json))
                        .map(jvs -> enrichMapper.enrich(jvs))
                        .map(JVS::getJsonNode)
                        .collect(Collectors.toList());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", "iterator-to-stream bridge");
        result.put("pipeline", "enrich");
        result.put("tags", tagArray);
        result.put("description", "Uses JSONIterator.toStream() to bridge hitorro iterators into Java Streams API");
        result.put("documentCount", results.size());
        result.put("timeMs", System.currentTimeMillis() - start);
        result.put("documents", results);
        return result;
    }

    // ─── Stream -> fromStream() -> sink example ───────────────────

    public Map<String, Object> streamToIteratorSinkExample(String tags) throws Exception {
        long start = System.currentTimeMillis();
        String[] tagArray = (tags != null && !tags.isEmpty())
                ? tags.split(",") : new String[]{"basic", "segmented", "ner"};

        Path inputPath = getDatasetPath();
        Path outputPath = inputPath.resolveSibling("demo-documents-stream-to-sink.ndjson");
        JvsEnrichMapper enrichMapper = new JvsEnrichMapper(tagArray);

        List<JsonNode> inputDocs = readDataset();

        // Create a Java stream, then bridge back to hitorro iterator with fromStream(), and sink
        Stream<JsonNode> enrichedStream = inputDocs.stream()
                .map(json -> new JVS(json))
                .map(jvs -> enrichMapper.enrich(jvs))
                .map(JVS::getJsonNode);

        int count;
        try (OutputStream os = Files.newOutputStream(outputPath)) {
            JsonSink sink = new JsonSink(os);
            // Bridge from Java Stream back to hitorro iterator and use sink
            count = AbstractIterator.fromStream(enrichedStream).sink(sink);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", "stream-to-iterator-sink bridge");
        result.put("pipeline", "enrich");
        result.put("tags", tagArray);
        result.put("description", "Uses AbstractIterator.fromStream() to bridge Java Streams back to hitorro iterators, then writes via JsonSink");
        result.put("outputFile", outputPath.toString());
        result.put("documentCount", count);
        result.put("timeMs", System.currentTimeMillis() - start);
        return result;
    }

    // ─── Helpers ──────────────────────────────────────────────────

    JVS2JVSTranslationMapper buildTranslationMapper(String sourceLang, List<String> targetLangs) {
        return JVS2JVSTranslationMapper.builder()
                .translator(jvsService::callOllamaTranslate)
                .sourceLanguage(sourceLang)
                .targetLanguages(targetLangs)
                .mlsFields("title.mls", "body.mls")
                .build();
    }

    private void writeNdjsonFile(Path path, List<JsonNode> docs) throws IOException {
        try (OutputStream os = Files.newOutputStream(path)) {
            JsonSink sink = new JsonSink(os);
            sink.start();
            for (JsonNode doc : docs) {
                sink.add(doc);
            }
            sink.stop();
        }
    }
}
