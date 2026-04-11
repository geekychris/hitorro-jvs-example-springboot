package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/jvs/pipeline")
@CrossOrigin(origins = "*")
public class NdjsonPipelineController {

    private final NdjsonPipelineService pipelineService;

    public NdjsonPipelineController(NdjsonPipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    // ─── Dataset ──────────────────────────────────────────────────

    @GetMapping("/dataset")
    public ResponseEntity<List<JsonNode>> getDataset() {
        try {
            return ResponseEntity.ok(pipelineService.readDataset());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/dataset/count")
    public ResponseEntity<Map<String, Object>> getDatasetCount() {
        try {
            List<JsonNode> docs = pipelineService.readDataset();
            return ResponseEntity.ok(Map.of(
                    "count", docs.size(),
                    "file", pipelineService.getDatasetPath().toString()));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("count", 0, "error", e.getMessage()));
        }
    }

    // ─── Enrich Pipelines ─────────────────────────────────────────

    @PostMapping("/enrich/iterator")
    public ResponseEntity<Map<String, Object>> enrichIterator(@RequestBody(required = false) JsonNode request) {
        try {
            String tags = extractTags(request);
            return ResponseEntity.ok(pipelineService.enrichPipelineIterator(tags));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/enrich/stream")
    public ResponseEntity<Map<String, Object>> enrichStream(@RequestBody(required = false) JsonNode request) {
        try {
            String tags = extractTags(request);
            return ResponseEntity.ok(pipelineService.enrichPipelineStream(tags));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ─── Translate Pipelines ──────────────────────────────────────

    @PostMapping("/translate/iterator")
    public ResponseEntity<Map<String, Object>> translateIterator(@RequestBody(required = false) JsonNode request) {
        try {
            String sourceLang = extractSourceLang(request);
            List<String> targetLangs = extractTargetLangs(request);
            return ResponseEntity.ok(pipelineService.translatePipelineIterator(sourceLang, targetLangs));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/translate/stream")
    public ResponseEntity<Map<String, Object>> translateStream(@RequestBody(required = false) JsonNode request) {
        try {
            String sourceLang = extractSourceLang(request);
            List<String> targetLangs = extractTargetLangs(request);
            return ResponseEntity.ok(pipelineService.translatePipelineStream(sourceLang, targetLangs));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ─── Combined Pipelines ───────────────────────────────────────

    @PostMapping("/combined/iterator")
    public ResponseEntity<Map<String, Object>> combinedIterator(@RequestBody(required = false) JsonNode request) {
        try {
            String tags = extractTags(request);
            String sourceLang = extractSourceLang(request);
            List<String> targetLangs = extractTargetLangs(request);
            return ResponseEntity.ok(pipelineService.combinedPipelineIterator(tags, sourceLang, targetLangs));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/combined/stream")
    public ResponseEntity<Map<String, Object>> combinedStream(@RequestBody(required = false) JsonNode request) {
        try {
            String tags = extractTags(request);
            String sourceLang = extractSourceLang(request);
            List<String> targetLangs = extractTargetLangs(request);
            return ResponseEntity.ok(pipelineService.combinedPipelineStream(tags, sourceLang, targetLangs));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ─── Bridge Examples ──────────────────────────────────────────

    @PostMapping("/bridge/iterator-to-stream")
    public ResponseEntity<Map<String, Object>> iteratorToStream(@RequestBody(required = false) JsonNode request) {
        try {
            String tags = extractTags(request);
            return ResponseEntity.ok(pipelineService.iteratorToStreamExample(tags));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/bridge/stream-to-sink")
    public ResponseEntity<Map<String, Object>> streamToSink(@RequestBody(required = false) JsonNode request) {
        try {
            String tags = extractTags(request);
            return ResponseEntity.ok(pipelineService.streamToIteratorSinkExample(tags));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────

    private String extractTags(JsonNode request) {
        if (request != null && request.has("tags")) return request.get("tags").asText("");
        return "";
    }

    private String extractSourceLang(JsonNode request) {
        if (request != null && request.has("sourceLanguage")) return request.get("sourceLanguage").asText("en");
        return "en";
    }

    private List<String> extractTargetLangs(JsonNode request) {
        List<String> langs = new ArrayList<>();
        if (request != null && request.has("targetLanguages") && request.get("targetLanguages").isArray()) {
            request.get("targetLanguages").forEach(n -> langs.add(n.asText()));
        }
        return langs.isEmpty() ? List.of("de", "es") : langs;
    }
}
