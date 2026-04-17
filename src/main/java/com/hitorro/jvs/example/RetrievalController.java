package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.index.ExampleDatasets;
import com.hitorro.index.IndexManager;
import com.hitorro.index.config.IndexConfig;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.JsonTypeSystem;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.kvstore.DatabaseConfig;
import com.hitorro.kvstore.KVStore;
import com.hitorro.kvstore.RocksDBStore;
import com.hitorro.kvstore.TypedKVStore;
import com.hitorro.retrieval.RetrievalConfig;
import com.hitorro.retrieval.RetrievalResult;
import com.hitorro.retrieval.RetrievalService;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.pipeline.RetrievalPipeline;
import com.hitorro.retrieval.pipeline.RetrievalPipelineBuilder;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.util.core.iterator.AbstractIterator;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * REST controller demonstrating the hitorro-retrieval pipeline module.
 * Supports creating multiple named indexes, indexing documents into them,
 * and executing retrieval pipelines with configurable stages.
 */
@RestController
@RequestMapping("/api/retrieval")
@CrossOrigin(origins = "*")
public class RetrievalController {

    private static final Logger log = LoggerFactory.getLogger(RetrievalController.class);

    private final NdjsonPipelineService pipelineService;
    private final JvsService jvsService;
    private final IndexManager indexManager;
    private final Map<String, IndexMeta> indexMetas = new ConcurrentHashMap<>();

    // Optional KVStore for document retrieval demo
    private KVStore rawKvStore;
    private TypedKVStore<JsonNode> typedKvStore;

    public RetrievalController(NdjsonPipelineService pipelineService, JvsService jvsService) {
        this.pipelineService = pipelineService;
        this.jvsService = jvsService;
        this.indexManager = new IndexManager("en");
    }

    @PreDestroy
    public void cleanup() {
        try { indexManager.close(); } catch (IOException e) { log.warn("IndexManager close error", e); }
        if (typedKvStore != null) { try { typedKvStore.close(); } catch (Exception e) {} }
        if (rawKvStore != null) { try { rawKvStore.close(); } catch (Exception e) {} }
    }

    // ─── Status ──────────────────────────────────────────────────────

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("indexes", indexMetas.keySet().stream().sorted().toList());
        status.put("indexCount", indexMetas.size());
        status.put("kvStoreEnabled", typedKvStore != null);

        List<Map<String, Object>> details = new ArrayList<>();
        for (var entry : indexMetas.entrySet()) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("name", entry.getKey());
            info.put("documentCount", entry.getValue().documentCount);
            info.put("typeName", entry.getValue().typeName);
            details.add(info);
        }
        status.put("indexDetails", details);
        return ResponseEntity.ok(status);
    }

    // ─── Index Management ────────────────────────────────────────────

    @PostMapping("/indexes")
    public ResponseEntity<Map<String, Object>> createIndex(@RequestBody Map<String, String> body) {
        String name = body.getOrDefault("name", "default");
        String typeName = body.get("typeName");

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            if (indexManager.hasIndex(name)) {
                indexManager.clearIndex(name);
                result.put("action", "cleared existing index");
            } else {
                IndexConfig config = IndexConfig.inMemory().storeSource(true).build();
                Type type = null;
                if (typeName != null && jvsService.isTypeSystemAvailable()) {
                    type = JsonTypeSystem.getMe().getType(typeName);
                }
                indexManager.createIndex(name, config, type);
                result.put("action", "created new index");
            }
            indexMetas.put(name, new IndexMeta(typeName, 0));
            result.put("name", name);
            result.put("typeName", typeName);
            result.put("success", true);
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return ResponseEntity.ok(result);
    }

    @DeleteMapping("/indexes/{name}")
    public ResponseEntity<Map<String, Object>> deleteIndex(@PathVariable String name) {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            if (indexManager.hasIndex(name)) {
                indexManager.closeIndex(name);
                indexMetas.remove(name);
                result.put("deleted", true);
            } else {
                result.put("deleted", false);
                result.put("message", "Index not found: " + name);
            }
        } catch (Exception e) {
            result.put("deleted", false);
            result.put("error", e.getMessage());
        }
        return ResponseEntity.ok(result);
    }

    @GetMapping("/indexes")
    public ResponseEntity<List<Map<String, Object>>> listIndexes() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (var entry : indexMetas.entrySet()) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("name", entry.getKey());
            info.put("documentCount", entry.getValue().documentCount);
            info.put("typeName", entry.getValue().typeName);
            list.add(info);
        }
        return ResponseEntity.ok(list);
    }

    // ─── Index Documents ─────────────────────────────────────────────

    @PostMapping("/indexes/{name}/documents")
    public ResponseEntity<Map<String, Object>> indexDocuments(
            @PathVariable String name,
            @RequestBody JsonNode body) {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            if (!indexManager.hasIndex(name)) {
                result.put("error", "Index not found: " + name);
                return ResponseEntity.badRequest().body(result);
            }

            // body can be a single doc, array, or {"source": "dataset", "enrichTags": "basic,ner"}
            List<JVS> docs;
            String enrichTags = null;
            if (body.has("source") && "dataset".equals(body.get("source").asText())) {
                docs = pipelineService.readDataset().stream().map(JVS::new).toList();
                if (body.has("enrichTags")) enrichTags = body.get("enrichTags").asText();
            } else if (body.isArray()) {
                docs = new ArrayList<>();
                for (JsonNode node : body) {
                    docs.add(new JVS(node));
                }
            } else if (body.has("documents")) {
                docs = new ArrayList<>();
                for (JsonNode node : body.get("documents")) {
                    docs.add(new JVS(node));
                }
                if (body.has("enrichTags")) enrichTags = body.get("enrichTags").asText();
            } else {
                docs = List.of(new JVS(body));
            }

            // Enrich before indexing if tags specified
            List<JVS> processedDocs;
            if (enrichTags != null && !enrichTags.isBlank()) {
                String[] tags = enrichTags.split(",");
                JvsEnrichMapper enrichMapper = new JvsEnrichMapper(tags);
                processedDocs = new ArrayList<>();
                for (JVS doc : docs) {
                    processedDocs.add(enrichMapper.enrich(doc));
                }
            } else {
                processedDocs = new ArrayList<>(docs);
            }

            indexManager.indexDocuments(name, processedDocs);
            indexManager.commit(name);
            storeInKv(processedDocs);

            IndexMeta meta = indexMetas.get(name);
            if (meta != null) {
                meta.documentCount += docs.size();
            }

            result.put("indexed", docs.size());
            result.put("indexName", name);
            result.put("success", true);
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return ResponseEntity.ok(result);
    }

    // ─── KVStore Toggle ──────────────────────────────────────────────

    @PostMapping("/kvstore/enable")
    public ResponseEntity<Map<String, Object>> enableKvStore() {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            if (typedKvStore == null) {
                File kvDir = new File("data/retrieval-kvstore");
                kvDir.mkdirs();
                DatabaseConfig dbConfig = DatabaseConfig.builder(kvDir.getAbsolutePath())
                        .createIfMissing(true).build();
                rawKvStore = new RocksDBStore(dbConfig);
                typedKvStore = new TypedKVStore<>(rawKvStore, JsonNode.class);
            }
            result.put("enabled", true);
        } catch (Exception e) {
            result.put("enabled", false);
            result.put("error", e.getMessage());
        }
        return ResponseEntity.ok(result);
    }

    @PostMapping("/kvstore/disable")
    public ResponseEntity<Map<String, Object>> disableKvStore() {
        if (typedKvStore != null) { try { typedKvStore.close(); } catch (Exception e) {} typedKvStore = null; }
        if (rawKvStore != null) { try { rawKvStore.close(); } catch (Exception e) {} rawKvStore = null; }
        return ResponseEntity.ok(Map.of("enabled", false));
    }

    // ─── Example Datasets ────────────────────────────────────────────

    @GetMapping("/datasets")
    public ResponseEntity<List<Map<String, Object>>> listDatasets() {
        List<Map<String, Object>> datasets = new ArrayList<>();
        for (ExampleDatasets.Dataset ds : ExampleDatasets.getAvailableDatasets()) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("name", ds.name());
            info.put("indexName", ds.getIndexName());
            info.put("documentCount", ExampleDatasets.getDocuments(ds).size());
            datasets.add(info);
        }
        // Also add demo_documents from NDJSON pipeline
        Map<String, Object> demo = new LinkedHashMap<>();
        demo.put("name", "DEMO_DOCUMENTS");
        demo.put("indexName", "demo_documents");
        try {
            demo.put("documentCount", pipelineService.readDataset().size());
        } catch (Exception e) {
            demo.put("documentCount", 0);
        }
        datasets.add(demo);
        return ResponseEntity.ok(datasets);
    }

    @PostMapping("/datasets/load")
    public ResponseEntity<Map<String, Object>> loadDataset(@RequestBody Map<String, Object> body) {
        String datasetName = (String) body.get("dataset");
        String enrichTags = (String) body.getOrDefault("enrichTags", null);
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            List<JVS> docs;
            String indexName;
            String typeName = null;

            if ("DEMO_DOCUMENTS".equals(datasetName)) {
                indexName = "demo_documents";
                typeName = "demo_document";
                docs = pipelineService.readDataset().stream().map(JVS::new).toList();
            } else {
                ExampleDatasets.Dataset ds = ExampleDatasets.Dataset.valueOf(datasetName);
                indexName = ds.getIndexName();
                docs = ExampleDatasets.getDocuments(ds);
                // Derive type name from the first document's type field
                if (!docs.isEmpty() && docs.get(0).exists("type")) {
                    typeName = docs.get(0).getString("type");
                }
            }

            // Create index if needed
            if (!indexManager.hasIndex(indexName)) {
                IndexConfig config = IndexConfig.inMemory().storeSource(true).build();
                Type type = null;
                if (typeName != null && jvsService.isTypeSystemAvailable()) {
                    type = JsonTypeSystem.getMe().getType(typeName);
                }
                indexManager.createIndex(indexName, config, type);
            }

            // Re-wrap documents through JVS constructor to trigger type resolution.
            // ExampleDatasets builds docs with set("type",...) which sets the JSON field
            // but does not resolve the internal Type object. Re-wrapping via new JVS(jsonNode)
            // triggers the constructor that looks up the type from the type system.
            List<JVS> resolvedDocs = new ArrayList<>();
            for (JVS doc : docs) {
                resolvedDocs.add(new JVS(doc.getJsonNode()));
            }

            // Enrich documents before indexing if tags are specified
            List<JVS> processedDocs;
            if (enrichTags != null && !enrichTags.isBlank()) {
                String[] tags = enrichTags.split(",");
                JvsEnrichMapper enrichMapper = new JvsEnrichMapper(tags);
                processedDocs = new ArrayList<>();
                int enrichedCount = 0;
                for (JVS doc : resolvedDocs) {
                    JVS enriched = enrichMapper.enrich(doc);
                    processedDocs.add(enriched);
                    if (enriched.getType() != null) enrichedCount++;
                }
                result.put("enriched", true);
                result.put("enrichTags", enrichTags);
                result.put("enrichedCount", enrichedCount);
                if (enrichedCount == 0 && !docs.isEmpty()) {
                    result.put("enrichWarning", "No documents were enriched. Check that documents have a 'type' field matching a loaded type definition (e.g. demo_article, demo_product, demo_document) and that the type system is available.");
                }
            } else {
                processedDocs = resolvedDocs;
                result.put("enriched", false);
            }

            // Index (this also computes id.id from domain:did) and store enriched form in KV
            indexManager.indexDocuments(indexName, processedDocs);
            indexManager.commit(indexName);
            storeInKv(processedDocs);
            indexMetas.put(indexName, new IndexMeta(typeName, processedDocs.size()));

            result.put("indexName", indexName);
            result.put("indexed", processedDocs.size());
            result.put("success", true);
        } catch (Exception e) {
            log.error("Dataset load failed", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return ResponseEntity.ok(result);
    }

    // ─── Multi-Index Search ─────────────────────────────────────────

    @PostMapping("/search-multiple")
    public ResponseEntity<Map<String, Object>> searchMultiple(@RequestBody JsonNode body) {
        Map<String, Object> result = new LinkedHashMap<>();
        long start = System.currentTimeMillis();

        try {
            List<String> indexNames = new ArrayList<>();
            if (body.has("indexNames") && body.get("indexNames").isArray()) {
                for (JsonNode n : body.get("indexNames")) {
                    String name = n.asText();
                    if (indexManager.hasIndex(name)) indexNames.add(name);
                }
            }
            if (indexNames.isEmpty()) {
                result.put("error", "No valid indexes specified");
                return ResponseEntity.badRequest().body(result);
            }

            String query = body.has("query") ? body.get("query").asText() : "*:*";
            int offset = body.has("offset") ? body.get("offset").asInt() : 0;
            int limit = body.has("limit") ? body.get("limit").asInt() : 20;
            String lang = body.has("lang") ? body.get("lang").asText() : "en";

            SearchResult sr = indexManager.searchMultiple(indexNames, query, offset, limit, lang);

            result.put("documents", sr.getDocuments().stream().map(JVS::getJsonNode).toList());
            result.put("documentCount", sr.getDocuments().size());
            result.put("totalHits", sr.getTotalHits());
            result.put("searchTimeMs", sr.getSearchTimeMs());
            result.put("indexNames", indexNames);
            result.put("success", true);
        } catch (Exception e) {
            log.error("Multi-index search failed", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        result.put("totalTimeMs", System.currentTimeMillis() - start);
        return ResponseEntity.ok(result);
    }

    // ─── Retrieval Pipeline ──────────────────────────────────────────

    @PostMapping("/execute")
    public ResponseEntity<Map<String, Object>> executeRetrieval(@RequestBody JsonNode body) {
        Map<String, Object> result = new LinkedHashMap<>();
        long start = System.currentTimeMillis();

        try {
            String indexName = body.has("indexName") ? body.get("indexName").asText() : null;
            if (indexName == null || !indexManager.hasIndex(indexName)) {
                result.put("error", "Index not found: " + indexName);
                return ResponseEntity.badRequest().body(result);
            }

            // Build the query JVS from the request
            JVS query = new JVS(body.get("query"));

            // Build pipeline config
            IndexMeta meta = indexMetas.get(indexName);
            Type type = null;
            if (meta != null && meta.typeName != null && jvsService.isTypeSystemAvailable()) {
                type = JsonTypeSystem.getMe().getType(meta.typeName);
            }
            String lang = body.has("lang") ? body.get("lang").asText() : "en";

            // Create retrieval service with optional KVStore
            RetrievalService service = typedKvStore != null
                    ? new RetrievalService(indexManager, typedKvStore)
                    : new RetrievalService(indexManager);

            RetrievalConfig config = new RetrievalConfig(indexName, type, lang);
            RetrievalResult retrievalResult = service.retrieve(config, query);

            // Collect results
            List<JVS> docs = retrievalResult.getDocumentList();
            List<JVS> aggregates = retrievalResult.getAggregates();

            result.put("documents", docs.stream().map(JVS::getJsonNode).toList());
            result.put("documentCount", docs.size());

            List<JsonNode> aggNodes = new ArrayList<>();
            for (JVS agg : aggregates) {
                if (agg != null) aggNodes.add(agg.getJsonNode());
            }
            result.put("aggregates", aggNodes);

            if (retrievalResult.hasErrors()) {
                result.put("errors", retrievalResult.getErrors());
            }

            // Include SearchResult metadata
            SearchResult sr = retrievalResult.getSearchResult();
            if (sr != null) {
                result.put("totalHits", sr.getTotalHits());
                result.put("searchTimeMs", sr.getSearchTimeMs());
                if (sr.hasFacets()) {
                    Map<String, Object> facetMap = new LinkedHashMap<>();
                    sr.getFacets().forEach((dim, fr) -> {
                        List<Map<String, Object>> values = new ArrayList<>();
                        fr.getValues().forEach(fv -> values.add(Map.of("value", fv.getValue(), "count", fv.getCount())));
                        facetMap.put(dim, Map.of("values", values, "totalCount", fr.getTotalCount()));
                    });
                    result.put("facets", facetMap);
                }
            }

            // Pipeline info
            List<String> stagesUsed = new ArrayList<>();
            stagesUsed.add("IndexRetriever");
            if (typedKvStore != null && body.get("query").has("fetch")) stagesUsed.add("DocumentRetriever");
            if (body.get("query").has("fixup")) stagesUsed.add("FixupRetriever");
            if (body.get("query").has("page")) stagesUsed.add("PaginationRetriever");
            if (sr != null && sr.hasFacets()) stagesUsed.add("FacetRetriever");
            result.put("stagesUsed", stagesUsed);

            result.put("success", true);
        } catch (Exception e) {
            log.error("Retrieval failed", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        result.put("totalTimeMs", System.currentTimeMillis() - start);
        return ResponseEntity.ok(result);
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    private void storeInKv(List<JVS> docs) {
        if (typedKvStore == null) return;
        for (JVS doc : docs) {
            String key = buildKey(doc);
            if (key != null) {
                typedKvStore.put(key, doc.getJsonNode());
            }
        }
    }

    private String buildKey(JVS doc) {
        try {
            if (doc.exists("id.domain") && doc.exists("id.did")) {
                return doc.getString("id.domain") + "/" + doc.getString("id.did");
            }
            if (doc.exists("id.did")) return doc.getString("id.did");
            // ExampleDatasets uses simple string id field
            if (doc.exists("id")) {
                String id = doc.getString("id");
                if (id != null && !id.isEmpty()) return id;
            }
        } catch (Exception e) {}
        return null;
    }

    static class IndexMeta {
        String typeName;
        int documentCount;
        IndexMeta(String typeName, int documentCount) {
            this.typeName = typeName;
            this.documentCount = documentCount;
        }
    }
}
