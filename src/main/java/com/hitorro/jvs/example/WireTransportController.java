package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.index.search.SearchResult;
import com.hitorro.index.stream.SearchResponseStream;
import com.hitorro.jsontypesystem.JVS;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Endpoints that match the wire transport protocol used by
 * {@link com.hitorro.retrieval.search.RemoteSearchProvider} and
 * {@link com.hitorro.retrieval.docstore.RemoteDocumentStore}.
 *
 * <p>These sit at {@code /api/search}, {@code /api/documents}, and {@code /api/health}
 * to match the URL patterns the remote providers construct.
 */
@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class WireTransportController {

    private final RetrievalController retrievalController;

    public WireTransportController(RetrievalController retrievalController) {
        this.retrievalController = retrievalController;
    }

    @GetMapping("/search")
    public ResponseEntity<Map<String, Object>> search(
            @RequestParam String index,
            @RequestParam String q,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(defaultValue = "en") String lang,
            @RequestParam(required = false) String facets) {
        return retrievalController.remoteSearch(index, q, offset, limit, lang, facets);
    }

    @GetMapping("/documents/{key}")
    public ResponseEntity<JsonNode> document(@PathVariable String key) {
        // Key may be URL-encoded (e.g., articles%2Fart-001 for articles/art-001)
        String decodedKey = java.net.URLDecoder.decode(key, java.nio.charset.StandardCharsets.UTF_8);
        return retrievalController.remoteDocument(decodedKey);
    }

    /**
     * NDJson streaming search endpoint for {@link com.hitorro.retrieval.search.StreamingRemoteSearchProvider}.
     * Writes one JSON document per line using chunked transfer encoding -- no full buffering.
     *
     * <p>Line 1: metadata (totalHits, searchTimeMs, query, offset, limit)
     * <p>Lines 2+: one document per line
     */
    @GetMapping(value = "/search/stream", produces = "application/x-ndjson")
    public ResponseEntity<StreamingResponseBody> searchStream(
            @RequestParam String index,
            @RequestParam String q,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(defaultValue = "en") String lang,
            @RequestParam(required = false) String facets) {

        StreamingResponseBody body = outputStream -> {
            try {
                var searchResponse = retrievalController.remoteSearch(index, q, offset, limit, lang, facets);
                var result = searchResponse.getBody();
                if (result == null) return;

                ObjectMapper mapper = new ObjectMapper();
                var writer = new java.io.OutputStreamWriter(outputStream, java.nio.charset.StandardCharsets.UTF_8);

                // Line 1: metadata
                Map<String, Object> meta = new java.util.LinkedHashMap<>();
                meta.put("_type", "meta");
                meta.put("totalHits", result.get("totalHits"));
                meta.put("searchTimeMs", result.get("searchTimeMs"));
                writer.write(mapper.writeValueAsString(meta));
                writer.write('\n');
                writer.flush();

                // Lines 2-N: one document per line
                Object docs = result.get("documents");
                if (docs instanceof List<?> docList) {
                    for (Object doc : docList) {
                        if (doc instanceof java.util.Map<?,?> docMap) {
                            ((java.util.Map) docMap).putIfAbsent("_type", "doc");
                        }
                        writer.write(mapper.writeValueAsString(doc));
                        writer.write('\n');
                        writer.flush();
                    }
                }

                // Tail: facets after all documents
                Object facetsObj = result.get("facets");
                if (facetsObj != null) {
                    Map<String, Object> facetLine = new java.util.LinkedHashMap<>();
                    facetLine.put("_type", "facets");
                    facetLine.putAll((java.util.Map<String, Object>) facetsObj);
                    writer.write(mapper.writeValueAsString(facetLine));
                    writer.write('\n');
                    writer.flush();
                }
            } catch (Exception e) {
                // Stream already started, can't change status code
            }
        };

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/x-ndjson"))
                .body(body);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return retrievalController.health();
    }
}
