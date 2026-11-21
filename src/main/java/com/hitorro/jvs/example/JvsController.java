package com.hitorro.jvs.example;

import tools.jackson.databind.JsonNode;
import com.hitorro.util.json.JsonMappers;
import com.hitorro.jsontypesystem.JVS;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import com.hitorro.util.json.JsonValues;

@RestController
@RequestMapping("/api/jvs")
@CrossOrigin(origins = "*")
public class JvsController {

    private final JvsService svc;

    public JvsController(JvsService svc) { this.svc = svc; }

    /**
     * The node as compact JSON. Not {@code node.toString()}: on Jackson 3 that fails past 500 levels of
     * nesting, while request bodies (and Jackson 2's toString) go to 1000.
     */
    private static String json(JsonNode node) {
        return JsonMappers.json().writeValueAsString(node);
    }

    // ─── Type System ───────────────────────────────────────────────

    @GetMapping("/types")
    public ResponseEntity<List<String>> listTypes() {
        return ResponseEntity.ok(svc.listTypes());
    }

    @GetMapping("/types/{typeName}")
    public ResponseEntity<Map<String, Object>> getType(@PathVariable String typeName) {
        Map<String, Object> def = svc.getTypeDefinition(typeName);
        return def != null ? ResponseEntity.ok(def) : ResponseEntity.notFound().build();
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
            "typeSystem", svc.isTypeSystemAvailable(),
            "types", svc.listTypes().size(),
            "translation", svc.getTranslationStatus()
        ));
    }

    // ─── Propaccess Path Navigation ──────────────────────────────────

    @PostMapping("/propaccess")
    public ResponseEntity<Map<String, Object>> propaccess(@RequestBody JsonNode request) {
        String json = request.has("json") && request.get("json").isString()
            ? JsonValues.asText(request.get("json")) : request.has("json") ? json(request.get("json")) : json(request);
        String path = request.has("path") ? JsonValues.asText(request.get("path"), "") : "";
        return ResponseEntity.ok(svc.getValueAtPath(json, path));
    }

    // ─── Documents ─────────────────────────────────────────────────

    @PostMapping("/documents")
    public ResponseEntity<JsonNode> createDocument(@RequestBody JsonNode body) {
        return ResponseEntity.ok(svc.createDocument(json(body)).getJsonNode());
    }

    @PostMapping("/merge")
    public ResponseEntity<JsonNode> merge(@RequestBody JsonNode request) {
        JVS merged = svc.mergeDocuments(
            json(request.get("base")), json(request.get("overlay")));
        return ResponseEntity.ok(merged.getJsonNode());
    }

    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validate(@RequestBody JsonNode body) {
        return ResponseEntity.ok(svc.validateDocument(json(body)));
    }

    // ─── Enrichment (Dynamic Fields) ───────────────────────────────

    @PostMapping("/enrich")
    public ResponseEntity<Map<String, Object>> enrich(@RequestBody JsonNode request) {
        String json = request.has("json") ? json(request.get("json")) : json(request);
        // If the request has a "json" field it's a structured request, otherwise raw JSON
        if (request.has("json") && request.get("json").isString()) {
            json = JsonValues.asText(request.get("json"));
        } else if (request.has("json")) {
            json = json(request.get("json"));
        } else {
            json = json(request);
        }
        String tags = request.has("tags") ? JsonValues.asText(request.get("tags"), "") : "";
        return ResponseEntity.ok(svc.enrichDocument(json, tags));
    }

    // ─── NLP ───────────────────────────────────────────────────────

    @PostMapping("/stem")
    public ResponseEntity<Map<String, String>> stem(@RequestBody Map<String, String> request) {
        String text = request.getOrDefault("text", "");
        String lang = request.getOrDefault("language", "en");
        return ResponseEntity.ok(Map.of("original", text, "stemmed", svc.stemText(text, lang), "language", lang));
    }

    // ─── Translation (Ollama AI) ───────────────────────────────────

    @PostMapping("/translate")
    public ResponseEntity<Map<String, Object>> translate(@RequestBody JsonNode request) {
        String json = request.has("json") && request.get("json").isString()
            ? JsonValues.asText(request.get("json")) : json(request.get("json"));
        List<String> mlsFields = new java.util.ArrayList<>();
        if (request.has("mlsFields")) {
            request.get("mlsFields").forEach(f -> mlsFields.add(JsonValues.asText(f)));
        }
        String sourceLang = request.has("sourceLanguage") ? JsonValues.asText(request.get("sourceLanguage"), "en") : "en";
        List<String> targetLangs = new java.util.ArrayList<>();
        if (request.has("targetLanguages")) {
            request.get("targetLanguages").forEach(l -> targetLangs.add(JsonValues.asText(l)));
        }
        return ResponseEntity.ok(svc.translateDocument(json, mlsFields, sourceLang, targetLangs));
    }

    @GetMapping("/translate/status")
    public ResponseEntity<Map<String, Object>> translationStatus() {
        return ResponseEntity.ok(svc.getTranslationStatus());
    }
}
