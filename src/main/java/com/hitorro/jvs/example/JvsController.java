package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/jvs")
@CrossOrigin(origins = "*")
public class JvsController {

    private final JvsService svc;

    public JvsController(JvsService svc) { this.svc = svc; }

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

    // ─── Documents ─────────────────────────────────────────────────

    @PostMapping("/documents")
    public ResponseEntity<JsonNode> createDocument(@RequestBody JsonNode body) {
        return ResponseEntity.ok(svc.createDocument(body.toString()).getJsonNode());
    }

    @PostMapping("/merge")
    public ResponseEntity<JsonNode> merge(@RequestBody JsonNode request) {
        JVS merged = svc.mergeDocuments(
            request.get("base").toString(), request.get("overlay").toString());
        return ResponseEntity.ok(merged.getJsonNode());
    }

    @PostMapping("/validate")
    public ResponseEntity<Map<String, Object>> validate(@RequestBody JsonNode body) {
        return ResponseEntity.ok(svc.validateDocument(body.toString()));
    }

    // ─── Enrichment (Dynamic Fields) ───────────────────────────────

    @PostMapping("/enrich")
    public ResponseEntity<Map<String, Object>> enrich(@RequestBody JsonNode request) {
        String json = request.has("json") ? request.get("json").toString() : request.toString();
        // If the request has a "json" field it's a structured request, otherwise raw JSON
        if (request.has("json") && request.get("json").isTextual()) {
            json = request.get("json").asText();
        } else if (request.has("json")) {
            json = request.get("json").toString();
        } else {
            json = request.toString();
        }
        String tags = request.has("tags") ? request.get("tags").asText("") : "";
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
        String json = request.has("json") && request.get("json").isTextual()
            ? request.get("json").asText() : request.get("json").toString();
        List<String> mlsFields = new java.util.ArrayList<>();
        if (request.has("mlsFields")) {
            request.get("mlsFields").forEach(f -> mlsFields.add(f.asText()));
        }
        String sourceLang = request.has("sourceLanguage") ? request.get("sourceLanguage").asText("en") : "en";
        List<String> targetLangs = new java.util.ArrayList<>();
        if (request.has("targetLanguages")) {
            request.get("targetLanguages").forEach(l -> targetLangs.add(l.asText()));
        }
        return ResponseEntity.ok(svc.translateDocument(json, mlsFields, sourceLang, targetLangs));
    }

    @GetMapping("/translate/status")
    public ResponseEntity<Map<String, Object>> translationStatus() {
        return ResponseEntity.ok(svc.getTranslationStatus());
    }
}
