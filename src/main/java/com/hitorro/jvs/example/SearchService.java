package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.index.IndexManager;
import com.hitorro.index.config.IndexConfig;
import com.hitorro.index.search.FacetResult;
import com.hitorro.index.search.SearchResult;
import com.hitorro.index.config.LuceneFieldType;
import com.hitorro.index.config.LuceneFieldTypes;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.JVS2JVSTranslationMapper;
import com.hitorro.jsontypesystem.JsonTypeSystem;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.kvstore.Result;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

@Service
public class SearchService {

    private static final Logger log = LoggerFactory.getLogger(SearchService.class);
    private static final String INDEX_NAME = "documents";

    private final NdjsonPipelineService pipelineService;
    private final JvsService jvsService;
    private final DocumentStoreService docStore;
    private final IndexManager indexManager;
    private boolean indexed = false;
    private String lastEnrichTags = null;
    private String lastTargetLangs = null;

    public SearchService(NdjsonPipelineService pipelineService, JvsService jvsService, DocumentStoreService docStore) {
        this.pipelineService = pipelineService;
        this.jvsService = jvsService;
        this.docStore = docStore;
        this.indexManager = new IndexManager("en");
    }

    @PreDestroy
    public void cleanup() {
        try {
            indexManager.close();
        } catch (IOException e) {
            log.warn("Error closing IndexManager", e);
        }
    }

    // ─── Index Dataset ────────────────────────────────────────────

    /**
     * Index the demo dataset with a stream pipeline: Load → Translate → Enrich → Index + KVStore.
     * Translation adds multi-language MLS entries, then enrichment computes NLP dynamic fields
     * (segmented, NER, POS, etc.) across all languages. The fully processed documents are
     * stored in both the Lucene index and RocksDB KVStore.
     */
    public Map<String, Object> indexDataset(String enrichTags, String targetLangs) throws Exception {
        long start = System.currentTimeMillis();

        // Load the Type for type-aware field mapping
        Type type = null;
        try {
            type = JsonTypeSystem.getMe().getType("demo_document");
        } catch (Exception e) {
            log.warn("Type system not available: {}", e.getMessage());
        }

        // Create or clear the index
        if (indexManager.hasIndex(INDEX_NAME)) {
            indexManager.clearIndex(INDEX_NAME);
        } else {
            IndexConfig config = IndexConfig.builder().inMemory().storeSource(false).build();
            indexManager.createIndex(INDEX_NAME, config, type);
        }

        // Build the translation stage (identity if no target languages)
        UnaryOperator<JVS> translateStage = UnaryOperator.identity();
        boolean doTranslate = targetLangs != null && !targetLangs.isEmpty();
        if (doTranslate) {
            List<String> langList = Arrays.stream(targetLangs.split(","))
                    .map(String::trim).filter(s -> !s.isEmpty()).toList();
            if (!langList.isEmpty()) {
                JVS2JVSTranslationMapper translationMapper = JVS2JVSTranslationMapper.builder()
                        .translator(jvsService::callOllamaTranslate)
                        .sourceLanguage("en")
                        .targetLanguages(langList)
                        .mlsFields("title.mls", "body.mls")
                        .build();
                translateStage = doc -> { translationMapper.apply(doc); return doc; };
            } else {
                doTranslate = false;
            }
        }

        // Build the enrichment stage (identity if no enrich tags)
        boolean doEnrich = enrichTags != null && !enrichTags.isEmpty();
        UnaryOperator<JVS> enrichStage = UnaryOperator.identity();
        if (doEnrich) {
            JvsEnrichMapper enrichMapper = new JvsEnrichMapper(enrichTags.split(","));
            enrichStage = doc -> doc.getType() != null ? enrichMapper.enrich(doc) : doc;
        }

        // Stream pipeline: Load → JVS → Translate → Enrich → Collect
        final UnaryOperator<JVS> translate = translateStage;
        final UnaryOperator<JVS> enrich = enrichStage;

        List<JVS> processedDocs = pipelineService.readDataset().stream()
                .map(JVS::new)
                .map(translate)
                .map(enrich)
                .collect(Collectors.toList());

        log.info("Pipeline complete: {} docs (translate={}, enrich={})", processedDocs.size(), doTranslate, doEnrich);

        // Store processed documents in KVStore
        int stored = 0;
        if (docStore.isOpen()) {
            for (JVS doc : processedDocs) {
                String key = docStore.buildKey(doc.getJsonNode());
                if (key != null) {
                    Result<Void> r = docStore.put(key, doc.getJsonNode());
                    if (r.isSuccess()) stored++;
                }
            }
        }

        // Index processed documents (enriched fields are searchable, _source has full doc)
        indexManager.indexDocuments(INDEX_NAME, processedDocs);
        indexManager.commit(INDEX_NAME);
        indexed = true;

        // Update state
        lastTargetLangs = doTranslate ? targetLangs : null;
        lastEnrichTags = doEnrich ? enrichTags : null;

        long elapsed = System.currentTimeMillis() - start;
        log.info("Indexed {} documents in {}ms (translated={}, enriched={}, kvStored={})",
                processedDocs.size(), elapsed, doTranslate, doEnrich, stored);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("indexName", INDEX_NAME);
        result.put("documentCount", processedDocs.size());
        result.put("indexTimeMs", elapsed);
        result.put("typeAware", type != null);
        if (type != null) result.put("typeName", type.getName());
        result.put("translated", doTranslate);
        if (doTranslate) result.put("targetLangs", targetLangs);
        result.put("enriched", doEnrich);
        if (doEnrich) result.put("enrichTags", enrichTags);
        result.put("storedInKV", stored);
        return result;
    }

    // ─── Search ───────────────────────────────────────────────────

    public Map<String, Object> search(String query, int offset, int limit, List<String> facetDims,
                                       String lang, boolean useKvStore) throws Exception {
        if (!indexed || !indexManager.hasIndex(INDEX_NAME)) {
            return Map.of("error", "Index not created. Call POST /api/jvs/search/index first.");
        }

        if (facetDims == null || facetDims.isEmpty()) {
            facetDims = List.of("department", "classification");
        }

        SearchResult result = indexManager.search(INDEX_NAME, query, offset, limit, facetDims, lang);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("query", query);
        response.put("totalHits", result.getTotalHits());
        response.put("offset", offset);
        response.put("limit", limit);
        response.put("searchTimeMs", result.getSearchTimeMs());
        response.put("kvStoreAvailable", docStore.isOpen());

        // Determine source: if useKvStore requested and KV store is available, fetch from KV
        boolean fromKvStore = useKvStore && docStore.isOpen();
        response.put("source", fromKvStore ? "kvstore" : "index");

        List<JsonNode> docs;
        if (fromKvStore) {
            // Extract doc IDs from Lucene results, fetch full documents from KV store
            docs = new ArrayList<>();
            int kvHits = 0;
            int kvMisses = 0;
            for (JVS jvs : result.getDocuments()) {
                // Try building key from full id structure first, then from id.id (domain:did format)
                String key = docStore.buildKey(jvs.getJsonNode());
                if (key == null) {
                    // Without _source, id.domain/id.did may not exist — try id.id (format: "domain:did")
                    JsonNode idNode = jvs.getJsonNode().get("id");
                    if (idNode != null && idNode.has("id")) {
                        String combinedId = idNode.get("id").asText();
                        int colon = combinedId.indexOf(':');
                        if (colon > 0) {
                            key = combinedId.substring(0, colon) + "/" + combinedId.substring(colon + 1);
                        }
                    }
                }
                if (key != null) {
                    Result<JsonNode> kvResult = docStore.get(key);
                    if (kvResult.isSuccess() && kvResult.getOrDefault(null) != null) {
                        docs.add(kvResult.getOrDefault(null));
                        kvHits++;
                        continue;
                    }
                }
                // Fallback to index doc if KV lookup fails
                docs.add(jvs.getJsonNode());
                kvMisses++;
            }
            if (kvMisses > 0) {
                response.put("kvMisses", kvMisses);
            }
        } else {
            docs = result.getDocuments().stream()
                    .map(JVS::getJsonNode)
                    .collect(Collectors.toList());
        }
        response.put("documents", docs);

        // Include facets if present
        if (result.hasFacets()) {
            Map<String, Object> facets = new LinkedHashMap<>();
            for (Map.Entry<String, FacetResult> entry : result.getFacets().entrySet()) {
                Map<String, Object> facetInfo = new LinkedHashMap<>();
                facetInfo.put("totalCount", entry.getValue().getTotalCount());
                List<Map<String, Object>> values = entry.getValue().getValues().stream()
                        .map(fv -> {
                            Map<String, Object> m = new LinkedHashMap<>();
                            m.put("value", fv.getValue());
                            m.put("count", fv.getCount());
                            return m;
                        })
                        .collect(Collectors.toList());
                facetInfo.put("values", values);
                facets.put(entry.getKey(), facetInfo);
            }
            response.put("facets", facets);
        }

        return response;
    }

    // ─── Fetch from KVStore ───────────────────────────────────────

    public Map<String, Object> fetchDocument(String key) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("key", key);
        if (!docStore.isOpen()) {
            response.put("error", "KVStore not available");
            return response;
        }
        Result<JsonNode> result = docStore.get(key);
        if (result.isSuccess() && result.getOrDefault(null) != null) {
            response.put("found", true);
            response.put("document", result.getOrDefault(null));
        } else {
            response.put("found", false);
        }
        return response;
    }

    // ─── Indexed Fields ────────────────────────────────────────────

    public List<Map<String, Object>> getIndexedFields(String typeName, String lang) {
        if (lang == null || lang.isEmpty()) lang = "en";
        Type type = JsonTypeSystem.getMe().getType(typeName);
        if (type == null) return List.of();

        List<Map<String, Object>> results = new ArrayList<>();
        LuceneFieldTypes fieldTypes = LuceneFieldTypes.getInstance();
        collectIndexedFields(type, "", lang, fieldTypes, results, 0);
        return results;
    }

    private void collectIndexedFields(Type type, String prefix, String lang,
                                       LuceneFieldTypes fieldTypes,
                                       List<Map<String, Object>> results, int depth) {
        if (depth > 10 || type == null || type.isPrimitiveType()) return;

        JsonNode metaNode = type.getMetaNode();
        if (metaNode == null || !metaNode.has("fields")) return;

        for (JsonNode fn : metaNode.get("fields")) {
            String fieldName = fn.has("name") ? fn.get("name").asText() : null;
            if (fieldName == null) continue;

            String fieldPath = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;
            boolean isVector = fn.has("vector") && fn.get("vector").asBoolean();
            boolean isDynamic = fn.has("dynamic");

            if (fn.has("groups") && fn.get("groups").isArray()) {
                for (JsonNode gn : fn.get("groups")) {
                    String groupName = gn.isTextual() ? gn.asText() : (gn.has("name") ? gn.get("name").asText() : "");
                    String method = gn.has("method") ? gn.get("method").asText() : "";

                    if ("index".equals(groupName) && !method.isEmpty()) {
                        LuceneFieldType lft = fieldTypes.get(method);
                        if (lft != null) {
                            StringBuilder physicalName = new StringBuilder(fieldPath);
                            lft.get(physicalName, lang, isVector);

                            Map<String, Object> entry = new LinkedHashMap<>();
                            entry.put("fieldPath", fieldPath);
                            entry.put("method", method);
                            entry.put("physicalName", physicalName.toString());
                            entry.put("i18n", lft.isI18n());
                            entry.put("tokenized", lft.isTokenized());
                            entry.put("dynamic", isDynamic);
                            results.add(entry);
                        }
                    }
                }
            }

            String fieldTypeName = fn.has("type") ? fn.get("type").asText() : null;
            if (fieldTypeName != null) {
                Type fieldType = JsonTypeSystem.getMe().getType(fieldTypeName);
                if (fieldType != null && !fieldType.isPrimitiveType()) {
                    collectIndexedFields(fieldType, fieldPath, lang, fieldTypes, results, depth + 1);
                }
            }
        }
    }

    // ─── Lucene Viewer ────────────────────────────────────────────

    public IndexManager getIndexManager() {
        return indexManager;
    }

    public boolean isIndexed() {
        return indexed;
    }

    // ─── Status ───────────────────────────────────────────────────

    public Map<String, Object> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("indexed", indexed);
        status.put("indexName", INDEX_NAME);
        status.put("kvStoreOpen", docStore.isOpen());
        if (lastEnrichTags != null) status.put("enrichTags", lastEnrichTags);
        if (lastTargetLangs != null) status.put("targetLangs", lastTargetLangs);
        return status;
    }
}
