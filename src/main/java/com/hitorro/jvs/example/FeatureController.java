/*
 * Copyright (c) 2006-2025 Chris Collins
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.features.*;
import com.hitorro.features.collections.BaseCollection;
import com.hitorro.features.collections.FeatureCollectionContext;
import com.hitorro.features.collections.concrete.SparseIntFC;
import com.hitorro.features.index.FeatureCacheManager;
import com.hitorro.features.index.FeatureIndexer;
import com.hitorro.features.index.FeatureQueryContext;
import com.hitorro.util.basefile.fs.BaseFile;
import com.hitorro.util.basefile.fs.file.FileFileSystem;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * REST controller demonstrating feature extraction, indexing, and querying.
 */
@RestController
@RequestMapping("/api/features")
@CrossOrigin(origins = "*")
public class FeatureController {

    record FeatureDefinition(String name, String dataType, String description, Feature feature) {}

    private final List<FeatureDefinition> demoFeatures;
    private final Map<Long, List<Map<String, Object>>> extractedStore = new LinkedHashMap<>();
    private Path indexDir;
    private FeatureCacheManager cacheManager;
    private boolean indexed = false;

    {
        // Initialize demo features and register them with the global FeatureEngine
        // so the index reader can resolve features by hash when reading back
        demoFeatures = createDemoFeatures();
        FeatureEngine engine = new FeatureEngine();
        for (FeatureDefinition fd : demoFeatures) {
            engine.addFeature(fd.feature());
        }
        FeatureEngine.setEngine(engine);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("featureCount", demoFeatures.size());
        status.put("extractedDocuments", extractedStore.size());
        status.put("indexed", indexed);
        status.put("features", demoFeatures.stream().map(f -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", f.name());
            m.put("dataType", f.dataType());
            m.put("description", f.description());
            return m;
        }).toList());
        return ResponseEntity.ok(status);
    }

    @PostMapping("/extract")
    public ResponseEntity<Map<String, Object>> extractFeatures(@RequestBody JsonNode document) {
        long docId = document.has("id") ? document.get("id").asLong(0) : Math.abs(document.hashCode());
        List<Map<String, Object>> features = extractFromDoc(document, docId);
        extractedStore.put(docId, features);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("documentId", docId);
        result.put("featureCount", features.size());
        result.put("features", features);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/extract-batch")
    public ResponseEntity<Map<String, Object>> extractBatch(@RequestBody JsonNode documents) {
        List<Map<String, Object>> docResults = new ArrayList<>();
        int totalFeatures = 0;

        if (documents.isArray()) {
            int idx = 0;
            for (JsonNode doc : documents) {
                long docId = doc.has("id") ? doc.get("id").asLong(0) : idx + 1;
                List<Map<String, Object>> features = extractFromDoc(doc, docId);
                extractedStore.put(docId, features);
                totalFeatures += features.size();

                Map<String, Object> docResult = new LinkedHashMap<>();
                docResult.put("documentId", docId);
                docResult.put("featureCount", features.size());
                docResult.put("features", features);
                docResults.add(docResult);
                idx++;
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("documentCount", docResults.size());
        result.put("totalFeatures", totalFeatures);
        result.put("documents", docResults);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/index")
    public synchronized ResponseEntity<Map<String, Object>> indexFeatures() throws IOException {
        if (extractedStore.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "No extracted features to index. Extract documents first."));
        }

        if (indexDir == null) {
            indexDir = Files.createTempDirectory("hitorro-features-idx");
        }

        FileFileSystem ffs = new FileFileSystem(indexDir.toFile());
        BaseFile indexDirectory = ffs.getFile("index");
        indexDirectory.mkdir();

        List<FeatureValue> allValues = new ArrayList<>();
        // Build a lookup of Feature objects by name
        Map<String, Feature> featureByName = new HashMap<>();
        for (FeatureDefinition fd : demoFeatures) {
            featureByName.put(fd.name(), fd.feature());
        }

        for (var entry : extractedStore.entrySet()) {
            long docId = entry.getKey();
            for (var fv : entry.getValue()) {
                String featureName = (String) fv.get("name");
                Object value = fv.get("value");
                Feature f = featureByName.get(featureName);
                if (value instanceof Number num && f != null) {
                    allValues.add(FeatureValue.getFeature(f, docId, 0, num));
                }
            }
        }

        allValues.sort(FVComparator.IDComp);

        FeatureIndexer indexer = new FeatureIndexer(indexDirectory);
        int count = indexer.processIterator(allValues.iterator());

        cacheManager = new FeatureCacheManager(indexDirectory);
        indexed = true;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("indexedValues", count);
        result.put("documentsIndexed", extractedStore.size());
        return ResponseEntity.ok(result);
    }

    @GetMapping("/query")
    public ResponseEntity<Map<String, Object>> queryFeature(
            @RequestParam long docId,
            @RequestParam String feature) throws IOException {

        if (!indexed || cacheManager == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Index not built. Extract and index documents first."));
        }

        Feature f = null;
        for (FeatureDefinition fd : demoFeatures) {
            if (fd.name().equals(feature)) { f = fd.feature(); break; }
        }
        if (f == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Unknown feature: " + feature));
        }

        FeatureSetKey fsk = FeatureSetKey.getFeatureAux(f, RealmId.None, -1);
        FeatureQueryContext fqc = cacheManager.getQueryContext();
        BaseCollection bc = fqc.getCollection(fsk);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("documentId", docId);
        result.put("feature", feature);

        // Look up from the in-memory extracted store
        boolean found = false;
        Object value = null;
        List<Map<String, Object>> docFeatures = extractedStore.get(docId);
        if (docFeatures != null) {
            for (var fv : docFeatures) {
                if (feature.equals(fv.get("name"))) {
                    found = true;
                    value = fv.get("value");
                    break;
                }
            }
        }
        result.put("found", found);
        result.put("value", value);

        // Also show index status
        if (bc instanceof SparseIntFC sfc) {
            long[] keys = f.getKeyType().getKeyBag(null, docId);
            result.put("indexHit", sfc.exists(-1, keys, new FeatureCollectionContext()) != null);
            result.put("indexValue", sfc.getInteger(-1, keys, new FeatureCollectionContext()));
        }
        return ResponseEntity.ok(result);
    }

    @GetMapping("/extracted")
    public ResponseEntity<Map<String, Object>> getExtracted() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("documentCount", extractedStore.size());
        List<Map<String, Object>> docs = new ArrayList<>();
        for (var entry : extractedStore.entrySet()) {
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("documentId", entry.getKey());
            doc.put("features", entry.getValue());
            docs.add(doc);
        }
        result.put("documents", docs);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/clear")
    public ResponseEntity<Map<String, Object>> clear() {
        extractedStore.clear();
        indexed = false;
        cacheManager = null;
        return ResponseEntity.ok(Map.of("status", "cleared"));
    }

    // ─── Internal ────────────────────────────────────────────────

    private List<Map<String, Object>> extractFromDoc(JsonNode doc, long docId) {
        List<Map<String, Object>> features = new ArrayList<>();
        for (FeatureDefinition fd : demoFeatures) {
            Map<String, Object> fv = new LinkedHashMap<>();
            fv.put("name", fd.name());
            fv.put("dataType", fd.dataType());
            fv.put("description", fd.description());
            try {
                fv.put("value", extractValue(fd, doc));
            } catch (Exception e) {
                fv.put("value", null);
                fv.put("error", e.getMessage());
            }
            features.add(fv);
        }
        return features;
    }

    private Object extractValue(FeatureDefinition feature, JsonNode doc) {
        return switch (feature.name()) {
            case "field_count" -> { int c = 0; var fs = doc.fields(); while (fs.hasNext()) { fs.next(); c++; } yield c; }
            case "title_length" -> { JsonNode t = doc.findValue("title"); yield t != null ? t.asText("").length() : 0; }
            case "word_count" -> {
                String txt = doc.has("body") ? doc.get("body").asText("") : doc.has("text") ? doc.get("text").asText("") : doc.has("content") ? doc.get("content").asText("") : "";
                yield txt.isBlank() ? 0 : txt.split("\\s+").length;
            }
            case "has_date" -> (doc.has("date") || doc.has("created") || doc.has("modified") || doc.has("publishDate") || doc.has("timestamp")) ? 1 : 0;
            case "depth" -> computeDepth(doc, 0);
            case "has_array" -> { boolean a = false; var it = doc.fields(); while (it.hasNext()) { if (it.next().getValue().isArray()) { a = true; break; } } yield a ? 1 : 0; }
            default -> null;
        };
    }

    private int computeDepth(JsonNode node, int current) {
        if (!node.isObject() && !node.isArray()) return current;
        int max = current;
        var it = node.elements();
        while (it.hasNext()) max = Math.max(max, computeDepth(it.next(), current + 1));
        return max;
    }

    private List<FeatureDefinition> createDemoFeatures() {
        return List.of(
            makeDef("field_count", "Number of top-level fields"),
            makeDef("title_length", "Character length of title"),
            makeDef("word_count", "Word count of body/text/content"),
            makeDef("has_date", "Has a date field (1=yes, 0=no)"),
            makeDef("depth", "Max JSON nesting depth"),
            makeDef("has_array", "Has array fields (1=yes, 0=no)")
        );
    }

    private FeatureDefinition makeDef(String name, String description) {
        Feature f = new Feature();
        f.setName(name);
        f.setDataType(DataType.integerType);
        f.setFeatureCardinality(FeatureCardinality.single);
        f.setKeyType(FeatureKeyType.document);
        return new FeatureDefinition(name, "integer", description, f);
    }
}
