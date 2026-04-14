package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/jvs/search")
@CrossOrigin(origins = "*")
public class SearchController {

    private final SearchService searchService;

    public SearchController(SearchService searchService) {
        this.searchService = searchService;
    }

    // ─── Index Dataset ────────────────────────────────────────────

    @PostMapping("/index")
    public ResponseEntity<Map<String, Object>> indexDataset(@RequestBody(required = false) JsonNode request) {
        try {
            String enrichTags = null;
            String targetLangs = null;
            if (request != null) {
                if (request.has("enrichTags")) {
                    enrichTags = request.get("enrichTags").asText("");
                }
                if (request.has("targetLangs")) {
                    targetLangs = request.get("targetLangs").asText("");
                }
            }
            return ResponseEntity.ok(searchService.indexDataset(enrichTags, targetLangs));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ─── Search ───────────────────────────────────────────────────

    @GetMapping
    public ResponseEntity<Map<String, Object>> search(
            @RequestParam String q,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(required = false) List<String> facets,
            @RequestParam(defaultValue = "en") String lang,
            @RequestParam(defaultValue = "false") boolean useKvStore) {
        try {
            return ResponseEntity.ok(searchService.search(q, offset, limit, facets, lang, useKvStore));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ─── Fetch from KVStore ───────────────────────────────────────

    @GetMapping("/doc/{domain}/{did}")
    public ResponseEntity<Map<String, Object>> fetchDocument(
            @PathVariable String domain, @PathVariable String did) {
        return ResponseEntity.ok(searchService.fetchDocument(domain + "/" + did));
    }

    // ─── Indexed Fields ─────────────────────────────────────────────

    @GetMapping("/fields/{typeName}")
    public ResponseEntity<List<Map<String, Object>>> indexedFields(
            @PathVariable String typeName,
            @RequestParam(defaultValue = "en") String lang) {
        return ResponseEntity.ok(searchService.getIndexedFields(typeName, lang));
    }

    // ─── Status ───────────────────────────────────────────────────

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(searchService.getStatus());
    }
}
