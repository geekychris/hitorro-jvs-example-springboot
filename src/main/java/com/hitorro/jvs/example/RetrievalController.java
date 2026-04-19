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
import com.hitorro.jsontypesystem.datamapper.AIOperations;
import com.hitorro.retrieval.RetrievalConfig;
import com.hitorro.retrieval.RetrievalResult;
import com.hitorro.retrieval.RetrievalService;
import com.hitorro.retrieval.context.ContextAttributes;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.docstore.DocumentStore;
import com.hitorro.retrieval.docstore.LocalKVDocumentStore;
import com.hitorro.retrieval.merger.*;
import com.hitorro.retrieval.pipeline.RetrievalPipeline;
import com.hitorro.retrieval.pipeline.RetrievalPipelineBuilder;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.retrieval.cluster.NodeAddress;
import com.hitorro.retrieval.cluster.NodeRole;
import com.hitorro.retrieval.docstore.RemoteDocumentStore;
import com.hitorro.retrieval.search.CompositeSearchProvider;
import com.hitorro.retrieval.search.LuceneSearchProvider;
import com.hitorro.retrieval.search.RemoteSearchProvider;
import com.hitorro.retrieval.search.SearchProvider;
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
    private boolean useRemoteTransport = false;

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

    // ─── Searchable Fields (for query builder) ─────────────────────

    @GetMapping("/fields/{indexName}")
    public ResponseEntity<List<Map<String, Object>>> getSearchableFields(@PathVariable String indexName) {
        List<Map<String, Object>> fields = new ArrayList<>();
        IndexMeta meta = indexMetas.get(indexName);
        if (meta == null || meta.typeName == null || !jvsService.isTypeSystemAvailable()) {
            return ResponseEntity.ok(fields);
        }

        Type type = JsonTypeSystem.getMe().getType(meta.typeName);
        if (type == null) return ResponseEntity.ok(fields);

        // Collect fields from type and super types
        addFieldsFromType(fields, type, "");

        return ResponseEntity.ok(fields);
    }

    private void addFieldsFromType(List<Map<String, Object>> fields, Type type, String prefix) {
        if (type == null) return;

        // Add inherited fields from super type first
        Type superType = type.getSuper();
        if (superType != null) {
            addFieldsFromType(fields, superType, prefix);
        }

        // Add own fields
        var metaNode = type.getMetaNode();
        if (metaNode != null && metaNode.has("fields") && metaNode.get("fields").isArray()) {
            for (var fn : metaNode.get("fields")) {
                String name = fn.has("name") ? fn.get("name").asText() : null;
                if (name == null) continue;
                String fullPath = prefix.isEmpty() ? name : prefix + "." + name;
                String fieldType = fn.has("type") ? fn.get("type").asText() : "core_string";
                boolean isMls = "core_mls".equals(fieldType);
                boolean isVector = fn.has("vector") && fn.get("vector").asBoolean();

                // Check if field has index groups
                boolean indexed = false;
                String method = null;
                if (fn.has("groups") && fn.get("groups").isArray()) {
                    for (var g : fn.get("groups")) {
                        if ("index".equals(g.has("name") ? g.get("name").asText() : "")) {
                            indexed = true;
                            method = g.has("method") ? g.get("method").asText() : null;
                            break;
                        }
                    }
                }

                Map<String, Object> info = new LinkedHashMap<>();
                info.put("path", fullPath);
                info.put("type", fieldType);
                info.put("indexed", indexed);
                info.put("method", method);
                info.put("vector", isVector);
                info.put("mls", isMls);

                // Generate query syntax hint and sub-fields for MLS types
                if (isMls) {
                    info.put("querySyntax", fullPath + ".mls:search_term");
                    info.put("hint", "MLS field — searches clean (normalized) text. Language-aware.");
                    // Add searchable MLS sub-fields
                    List<Map<String, Object>> subFields = new ArrayList<>();
                    Type mlsElemType = JsonTypeSystem.getMe().getType("core_mlselem");
                    if (mlsElemType != null && mlsElemType.getMetaNode() != null
                            && mlsElemType.getMetaNode().has("fields")) {
                        for (var sf : mlsElemType.getMetaNode().get("fields")) {
                            String sfName = sf.has("name") ? sf.get("name").asText() : null;
                            if (sfName == null || "lang".equals(sfName)) continue;
                            boolean sfIndexed = false;
                            String sfMethod = null;
                            if (sf.has("groups") && sf.get("groups").isArray()) {
                                for (var sg : sf.get("groups")) {
                                    if ("index".equals(sg.has("name") ? sg.get("name").asText() : "")) {
                                        sfIndexed = true;
                                        sfMethod = sg.has("method") ? sg.get("method").asText() : null;
                                        break;
                                    }
                                }
                            }
                            boolean isDynamic = sf.has("dynamic");
                            Map<String, Object> sub = new LinkedHashMap<>();
                            sub.put("name", sfName);
                            sub.put("path", fullPath + ".mls." + sfName);
                            sub.put("indexed", sfIndexed);
                            sub.put("method", sfMethod);
                            sub.put("dynamic", isDynamic);
                            sub.put("querySyntax", sfIndexed ? fullPath + ".mls." + sfName + ":term" : null);
                            subFields.add(sub);
                        }
                    }
                    info.put("subFields", subFields);
                } else if (indexed && "identifier".equals(method)) {
                    info.put("querySyntax", fullPath + ":\"exact value\"");
                    info.put("hint", "Exact match (keyword). Use quotes for multi-word values.");
                } else if (indexed && "text".equals(method)) {
                    info.put("querySyntax", fullPath + ":search_term");
                    info.put("hint", "Full-text search. Supports AND, OR, phrases, wildcards.");
                } else if (indexed) {
                    info.put("querySyntax", fullPath + ":value");
                    info.put("hint", "Indexed field.");
                } else {
                    info.put("querySyntax", null);
                    info.put("hint", "Not indexed — stored only.");
                }

                fields.add(info);
            }
        }
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

            boolean useKvStore = body.has("useKvStore") && body.get("useKvStore").asBoolean();
            String mergerName = body.has("merger") ? body.get("merger").asText() : "score";

            // Extract facet dimensions
            List<String> facetDims = null;
            if (body.has("facets")) {
                String facetStr = body.get("facets").asText();
                if (facetStr != null && !facetStr.isBlank()) {
                    facetDims = List.of(facetStr.split(","));
                }
            }

            // Build per-index providers and merge via CompositeSearchProvider
            List<SearchProvider> providers = new ArrayList<>();
            if (useRemoteTransport) {
                // Wire transport: each index gets a RemoteSearchProvider
                NodeAddress self = new NodeAddress("localhost", "localhost", 8080, Set.of(NodeRole.INDEX));
                for (String idxName : indexNames) {
                    providers.add(new RemoteSearchProvider(self) {
                        @Override public SearchResult search(String ig, String q, int o, int l,
                                                   java.util.List<String> f, String ln) throws Exception {
                            return super.search(idxName, q, o, l, f, ln);
                        }
                        @Override public String getName() { return "remote:" + idxName; }
                    });
                }
            } else {
                for (String idxName : indexNames) {
                    final String idx = idxName;
                    providers.add(new LuceneSearchProvider(indexManager) {
                        @Override public SearchResult search(String ig, String q, int o, int l,
                                                   java.util.List<String> f, String ln) throws Exception {
                            return super.search(idx, q, o, l, f, ln);
                        }
                        @Override public String getName() { return "lucene:" + idx; }
                    });
                }
            }

            ResultMerger merger = selectMerger(mergerName);
            CompositeSearchProvider composite = new CompositeSearchProvider(providers, merger);
            SearchResult sr = composite.search("multi", query, offset, limit, facetDims, lang);

            // Fetch from KVStore if enabled
            List<JVS> docs = sr.getDocuments();
            if (useKvStore && typedKvStore != null) {
                docs = fetchFromKv(docs);
                result.put("source", "kvstore");
            } else {
                result.put("source", "index");
            }

            result.put("documents", docs.stream().map(JVS::getJsonNode).toList());
            result.put("documentCount", docs.size());
            result.put("totalHits", sr.getTotalHits());
            result.put("searchTimeMs", sr.getSearchTimeMs());
            result.put("indexNames", indexNames);
            result.put("merger", merger.getName());
            // Extract merged facets
            if (sr.hasFacets()) {
                Map<String, Object> facetMap = new LinkedHashMap<>();
                sr.getFacets().forEach((dim, fr) -> {
                    List<Map<String, Object>> values = new ArrayList<>();
                    fr.getValues().forEach(fv -> values.add(Map.of("value", fv.getValue(), "count", fv.getCount())));
                    facetMap.put(dim, Map.of("values", values, "totalCount", fr.getTotalCount()));
                });
                result.put("facets", facetMap);
            }
            result.put("searchProviders", providers.stream().map(SearchProvider::getName).toList());
            result.put("stagesUsed", List.of(
                    "CompositeSearchProvider (" + composite.getName() + ")",
                    "ResultMerger (" + merger.getName() + ")",
                    useKvStore ? "KVStore fetch" : "Index _source"
            ));
            // Surface context info
            Map<String, Object> attrs = new LinkedHashMap<>();
            attrs.put("searchProviders", composite.getName());
            attrs.put("merger", merger.getName());
            attrs.put("totalHits", sr.getTotalHits());
            if (useKvStore) attrs.put("documentStore", "local-kv");
            result.put("contextAttributes", attrs);
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

            // Create retrieval service with pluggable providers
            SearchProvider searchProvider;
            DocumentStore docStore;
            if (useRemoteTransport) {
                // Wire transport: HTTP calls to our own endpoints (demonstrating the protocol)
                NodeAddress self = new NodeAddress("localhost", "localhost", 8080, Set.of(NodeRole.INDEX, NodeRole.KVSTORE));
                searchProvider = new RemoteSearchProvider(self);
                docStore = typedKvStore != null ? new RemoteDocumentStore(self) : null;
            } else {
                searchProvider = new LuceneSearchProvider(indexManager);
                docStore = typedKvStore != null ? new LocalKVDocumentStore(typedKvStore) : null;
            }
            RetrievalService service = docStore != null
                    ? new RetrievalService(searchProvider, docStore)
                    : new RetrievalService(searchProvider);

            // Enable AI summarization if Ollama is available
            if (body.has("query") && body.get("query").has("summarize")) {
                service.enableSummarization(createAIOperations());
            }

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
            if (body.get("query").has("summarize")) stagesUsed.add("SummarizationRetriever");
            result.put("stagesUsed", stagesUsed);

            // Context attributes (inter-stage metadata)
            Map<String, Object> attrs = new LinkedHashMap<>();
            var ctx = retrievalResult.getContext();
            if (ctx.hasAttribute(ContextAttributes.SEARCH_PROVIDERS))
                attrs.put("searchProvider", ctx.getAttribute(ContextAttributes.SEARCH_PROVIDERS, String.class));
            if (ctx.hasAttribute(ContextAttributes.TOTAL_HITS))
                attrs.put("totalHits", ctx.getAttribute(ContextAttributes.TOTAL_HITS, Long.class));
            if (ctx.hasAttribute(ContextAttributes.DOCUMENT_STORE_TYPE))
                attrs.put("documentStore", ctx.getAttribute(ContextAttributes.DOCUMENT_STORE_TYPE, String.class));
            if (ctx.hasAttribute(ContextAttributes.AI_SUMMARY))
                attrs.put("aiSummary", ctx.getAttribute(ContextAttributes.AI_SUMMARY, String.class));
            if (!attrs.isEmpty()) result.put("contextAttributes", attrs);

            result.put("success", true);
        } catch (Exception e) {
            log.error("Retrieval failed", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        result.put("totalTimeMs", System.currentTimeMillis() - start);
        return ResponseEntity.ok(result);
    }

    // ─── Streaming Execute (NDJson unified stream) ────────────────

    /**
     * Execute retrieval pipeline and return results as NDJson unified stream.
     * Stream order: metadata, documents, facets, aggregates.
     * Documents can be consumed as they arrive; facets/aggregates follow at the tail.
     */
    @PostMapping(value = "/execute/stream", produces = "application/x-ndjson")
    public org.springframework.http.ResponseEntity<org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody>
    executeRetrievalStream(@RequestBody JsonNode body) {
        org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody streamBody = outputStream -> {
            try {
                String indexName = body.has("indexName") ? body.get("indexName").asText() : null;
                if (indexName == null || !indexManager.hasIndex(indexName)) return;

                JVS query = new JVS(body.get("query"));
                IndexMeta meta = indexMetas.get(indexName);
                Type type = null;
                if (meta != null && meta.typeName != null && jvsService.isTypeSystemAvailable()) {
                    type = JsonTypeSystem.getMe().getType(meta.typeName);
                }
                String lang = body.has("lang") ? body.get("lang").asText() : "en";

                SearchProvider searchProvider = useRemoteTransport
                        ? new com.hitorro.retrieval.search.RemoteSearchProvider(
                            new com.hitorro.retrieval.cluster.NodeAddress("localhost", "localhost", 8080, Set.of(com.hitorro.retrieval.cluster.NodeRole.INDEX)))
                        : new LuceneSearchProvider(indexManager);
                DocumentStore docStore = typedKvStore != null ? new LocalKVDocumentStore(typedKvStore) : null;
                RetrievalService service = docStore != null
                        ? new RetrievalService(searchProvider, docStore)
                        : new RetrievalService(searchProvider);
                if (body.has("query") && body.get("query").has("summarize")) {
                    service.enableSummarization(createAIOperations());
                }

                RetrievalConfig config = new RetrievalConfig(indexName, type, lang);
                RetrievalResult retrievalResult = service.retrieve(config, query);

                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                var writer = new java.io.OutputStreamWriter(outputStream, java.nio.charset.StandardCharsets.UTF_8);

                // Write metadata line
                SearchResult sr = retrievalResult.getSearchResult();
                if (sr != null) {
                    Map<String, Object> metaMap = new LinkedHashMap<>();
                    metaMap.put("_type", "meta");
                    metaMap.put("totalHits", sr.getTotalHits());
                    metaMap.put("searchTimeMs", sr.getSearchTimeMs());
                    writer.write(mapper.writeValueAsString(metaMap));
                    writer.write('\n');
                    writer.flush();
                }

                // Stream unified results: docs first, then facets/aggregates at tail
                var unified = retrievalResult.unifiedStream();
                while (unified.hasNext()) {
                    JVS item = unified.next();
                    writer.write(mapper.writeValueAsString(item.getJsonNode()));
                    writer.write('\n');
                    writer.flush();
                }
            } catch (Exception e) {
                log.error("Streaming execute failed", e);
            }
        };

        return org.springframework.http.ResponseEntity.ok()
                .contentType(org.springframework.http.MediaType.parseMediaType("application/x-ndjson"))
                .body(streamBody);
    }

    // ─── Index Viewer (for retrieval indexes) ──────────────────────

    private org.apache.lucene.index.DirectoryReader getReaderForIndex(String indexName) throws Exception {
        var handle = indexManager.getIndex(indexName);
        if (handle == null) throw new IllegalStateException("Index not found: " + indexName);
        return org.apache.lucene.index.DirectoryReader.open(handle.getWriter().getIndexWriter());
    }

    @GetMapping("/viewer/{indexName}/stats")
    public ResponseEntity<?> viewerStats(@PathVariable String indexName) {
        try {
            var reader = getReaderForIndex(indexName);
            var stats = com.hitorro.luceneviewer.LuceneViewer.stats(reader);
            reader.close();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/viewer/{indexName}/fields")
    public ResponseEntity<?> viewerFields(@PathVariable String indexName) {
        try {
            var reader = getReaderForIndex(indexName);
            var fields = com.hitorro.luceneviewer.LuceneViewer.listFields(reader);
            reader.close();
            return ResponseEntity.ok(fields);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/viewer/{indexName}/documents")
    public ResponseEntity<?> viewerDocuments(@PathVariable String indexName,
                                             @RequestParam(defaultValue = "0") int start,
                                             @RequestParam(defaultValue = "10") int limit) {
        try {
            var reader = getReaderForIndex(indexName);
            var docs = com.hitorro.luceneviewer.LuceneViewer.listDocuments(reader, start, limit, true, false);
            reader.close();
            return ResponseEntity.ok(docs);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/viewer/{indexName}/terms/{field}")
    public ResponseEntity<?> viewerTerms(@PathVariable String indexName,
                                          @PathVariable String field,
                                          @RequestParam(required = false) String after,
                                          @RequestParam(defaultValue = "50") int limit) {
        try {
            var reader = getReaderForIndex(indexName);
            var terms = com.hitorro.luceneviewer.LuceneViewer.listTerms(reader, field, after, limit);
            reader.close();
            return ResponseEntity.ok(terms);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/viewer/{indexName}/search")
    public ResponseEntity<?> viewerSearch(@PathVariable String indexName,
                                           @RequestParam String q,
                                           @RequestParam(defaultValue = "content") String field,
                                           @RequestParam(defaultValue = "10") int limit,
                                           @RequestParam(required = false) String pageToken) {
        try {
            var reader = getReaderForIndex(indexName);
            var searcher = new org.apache.lucene.search.IndexSearcher(reader);
            var result = com.hitorro.luceneviewer.LuceneViewer.search(searcher, q, field, limit, pageToken, true);
            reader.close();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ─── Helpers ─────────────────────────────────────────────────────

    // ─── Wire Transport ────────────────────────────────────────────

    /** Endpoint that RemoteSearchProvider calls. */
    @GetMapping("/search")
    public ResponseEntity<Map<String, Object>> remoteSearch(
            @RequestParam String index,
            @RequestParam String q,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(defaultValue = "en") String lang,
            @RequestParam(required = false) String facets) {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            List<String> facetDims = (facets != null && !facets.isBlank())
                    ? List.of(facets.split(",")) : null;
            SearchResult sr = indexManager.search(index, q, offset, limit, facetDims, lang);
            result.put("documents", sr.getDocuments().stream().map(JVS::getJsonNode).toList());
            result.put("totalHits", sr.getTotalHits());
            result.put("searchTimeMs", sr.getSearchTimeMs());
        } catch (Exception e) {
            result.put("error", e.getMessage());
        }
        return ResponseEntity.ok(result);
    }

    /** Endpoint that RemoteDocumentStore calls. */
    @GetMapping("/documents/{key}")
    public ResponseEntity<JsonNode> remoteDocument(@PathVariable String key) {
        if (typedKvStore == null) {
            return ResponseEntity.notFound().build();
        }
        var kvResult = typedKvStore.get(key);
        if (kvResult.isSuccess() && kvResult.getValue().isPresent()) {
            return ResponseEntity.ok(kvResult.getValue().get());
        }
        return ResponseEntity.notFound().build();
    }

    /** Health check for RemoteSearchProvider/RemoteDocumentStore availability detection. */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of("status", "ok", "indexes", indexMetas.size()));
    }

    @PostMapping("/transport/toggle")
    public ResponseEntity<Map<String, Object>> toggleTransport() {
        useRemoteTransport = !useRemoteTransport;
        return ResponseEntity.ok(Map.of("remote", useRemoteTransport,
                "description", useRemoteTransport
                        ? "Using wire transport (HTTP calls to localhost — same JVM, demonstrating the remote protocol)"
                        : "Using local in-process providers (direct Lucene + KVStore calls)"));
    }

    @GetMapping("/transport/status")
    public ResponseEntity<Map<String, Object>> transportStatus() {
        return ResponseEntity.ok(Map.of("remote", useRemoteTransport));
    }

    private ResultMerger selectMerger(String name) {
        return switch (name) {
            case "field" -> new FieldSortMerger();
            case "rrf" -> new RRFMerger();
            default -> new ScoreMerger();
        };
    }

    private List<JVS> fetchFromKv(List<JVS> docs) {
        List<JVS> kvDocs = new ArrayList<>();
        for (JVS doc : docs) {
            String key = buildKey(doc);
            if (key != null && typedKvStore != null) {
                var kvResult = typedKvStore.get(key);
                if (kvResult.isSuccess() && kvResult.getValue().isPresent()) {
                    JVS fullDoc = new JVS(kvResult.getValue().get());
                    if (doc.exists("_score")) fullDoc.set("_score", doc.get("_score"));
                    if (doc.exists("_index")) fullDoc.set("_index", doc.get("_index"));
                    if (doc.exists("_uid")) fullDoc.set("_uid", doc.get("_uid"));
                    kvDocs.add(fullDoc);
                    continue;
                }
            }
            kvDocs.add(doc);
        }
        return kvDocs;
    }

    /**
     * Creates an AIOperations that tries Ollama first, falls back to extractive summary.
     * Always reports isAvailable()=true so the stage always participates.
     */
    private AIOperations createAIOperations() {
        return new AIOperations() {
            @Override public String translate(String text, String src, String tgt) {
                return jvsService.callOllamaTranslate(text, src, tgt);
            }
            @Override public String summarize(String text, int maxWords) {
                // Try Ollama first
                try {
                    var status = jvsService.getTranslationStatus();
                    if (Boolean.TRUE.equals(status.get("available"))) {
                        String prompt = "Summarize the following search results in " + maxWords + " words or fewer. "
                                + "Focus on the key themes and important findings:\n\n" + text;
                        var body = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode();
                        body.put("model", (String) status.getOrDefault("model", "llama3.2"));
                        body.put("prompt", prompt);
                        body.put("stream", false);

                        var client = java.net.http.HttpClient.newHttpClient();
                        var request = java.net.http.HttpRequest.newBuilder()
                                .uri(java.net.URI.create(status.getOrDefault("url", "http://localhost:11434") + "/api/generate"))
                                .header("Content-Type", "application/json")
                                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body.toString()))
                                .build();
                        var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
                        var responseJson = new com.fasterxml.jackson.databind.ObjectMapper().readTree(response.body());
                        if (responseJson.has("response")) {
                            return "[Ollama] " + responseJson.get("response").asText().trim();
                        }
                    }
                } catch (Exception e) {
                    log.debug("Ollama unavailable, using extractive summary: {}", e.getMessage());
                }

                // Fallback: extractive summary (first N sentences from the input)
                return buildExtractiveSummary(text, maxWords);
            }
            @Override public String ask(String text, String question) { return null; }
            @Override public boolean isAvailable() { return true; } // always available via fallback
        };
    }

    /**
     * Simple extractive summary: extracts the first few document titles/sentences
     * from the concatenated text. No LLM needed.
     */
    private String buildExtractiveSummary(String text, int maxWords) {
        if (text == null || text.isBlank()) return "No content to summarize.";

        StringBuilder summary = new StringBuilder("[Extractive] ");
        String[] lines = text.split("\n");
        int wordCount = 0;
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("Document ")) continue;
            // Take the first meaningful content from each document
            String[] words = line.split("\\s+");
            for (String word : words) {
                if (wordCount >= maxWords) break;
                if (wordCount > 0) summary.append(' ');
                summary.append(word);
                wordCount++;
            }
            if (wordCount >= maxWords) break;
            summary.append(". ");
        }
        return summary.toString().trim();
    }

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
