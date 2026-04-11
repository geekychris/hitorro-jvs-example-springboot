package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.jsontypesystem.Field;
import com.hitorro.jsontypesystem.Group;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.JVSMerger;
import com.hitorro.jsontypesystem.JVSValidator;
import com.hitorro.jsontypesystem.JsonTypeSystem;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jsontypesystem.propreaders.JVSProperties;
import com.hitorro.util.core.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.*;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class JvsService {

    private static final Logger log = LoggerFactory.getLogger(JvsService.class);

    private boolean typeSystemAvailable = false;
    private final Map<String, SnowballStemmer> stemmers = new ConcurrentHashMap<>();
    private String ollamaUrl = "http://localhost:11434";
    private String ollamaModel = "llama3.2";

    public JvsService() {
        stemmers.put("en", new EnglishStemmer());
        stemmers.put("de", new GermanStemmer());
        stemmers.put("fr", new FrenchStemmer());
        stemmers.put("es", new SpanishStemmer());
        stemmers.put("nl", new DutchStemmer());
        stemmers.put("pt", new PortugueseStemmer());
        stemmers.put("it", new ItalianStemmer());
        stemmers.put("sv", new SwedishStemmer());
    }

    @PostConstruct
    public void init() {
        try {
            // Resolve HT_BIN for type system
            String htBin = System.getProperty("HT_BIN", System.getenv("HT_BIN"));
            if (htBin == null) {
                // Try relative paths from working directory
                // Check parent first (hitorro root) since the example app also has a local config/types
                for (String path : new String[]{"..", "."}) {
                    File dataDir = new File(path, "data");
                    File configDir = new File(path, "config/types");
                    if (configDir.isDirectory() && dataDir.isDirectory()) {
                        htBin = new File(path).getCanonicalPath();
                        break;
                    }
                }
            }
            if (htBin != null) {
                System.setProperty("HT_BIN", htBin);
                System.setProperty("HT_HOME", htBin);
                log.info("HT_BIN set to: {}", htBin);
            }

            // Initialize properties for both GlobalProperties (hitorro-core) and JVSProperties (jsontypesystem)
            com.fasterxml.jackson.databind.node.ObjectNode coreProps = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode();
            if (htBin != null) {
                coreProps.put("HT_BIN", htBin);
                coreProps.put("HT_HOME", htBin);
                coreProps.put("ht_bin", htBin);
                coreProps.put("ht_data", htBin + "/data");
            }
            com.hitorro.util.core.params.GlobalProperties.setDefaultProperties(coreProps);

            JVS props = new JVS(coreProps.deepCopy());
            JVSProperties.setDefaultProperties(props, false);

            // Initialize language table (requires ht_data for ISO 639 CSV)
            try {
                com.hitorro.language.Iso639Table.getInstance();
                log.info("Iso639Table initialized");
            } catch (Exception | Error e) {
                log.warn("Iso639Table init failed (NLP may be limited): {}", e.getMessage());
            }

            // Initialize the type system
            JsonTypeSystem.getMe();
            typeSystemAvailable = true;
            log.info("Type system initialized successfully");
        } catch (Exception | Error e) {
            log.error("Type system initialization failed: {}", e.getMessage(), e);
            typeSystemAvailable = false;
        }

        // Configure Ollama
        ollamaUrl = System.getProperty("ollama.url", System.getenv().getOrDefault("OLLAMA_URL", ollamaUrl));
        ollamaModel = System.getProperty("ollama.model", System.getenv().getOrDefault("OLLAMA_MODEL", ollamaModel));
    }

    // ─── Type System ───────────────────────────────────────────────

    public boolean isTypeSystemAvailable() { return typeSystemAvailable; }

    public List<String> listTypes() {
        List<String> types = new ArrayList<>();
        for (String path : new String[]{"config/types", "../config/types"}) {
            File typesDir = new File(path);
            if (typesDir.isDirectory()) {
                File[] files = typesDir.listFiles();
                if (files != null) {
                    for (File f : files) {
                        if (f.getName().endsWith(".json")) types.add(f.getName().replace(".json", ""));
                    }
                }
                break;
            }
        }
        Collections.sort(types);
        return types;
    }

    public Map<String, Object> getTypeDefinition(String typeName) {
        if (!typeSystemAvailable) return null;
        Type type = JsonTypeSystem.getMe().getType(typeName);
        if (type == null) return null;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("name", type.getName());
        Type superType = type.getSuper();
        if (superType != null) result.put("superType", superType.getName());
        result.put("primitive", type.isPrimitiveType());
        if (type.isPrimitiveType() && type.getPrimitiveType() != null) {
            result.put("primitiveType", type.getPrimitiveType().name());
        }

        List<Map<String, Object>> fields = new ArrayList<>();
        JsonNode metaNode = type.getMetaNode();
        if (metaNode != null && metaNode.has("fields") && metaNode.get("fields").isArray()) {
            for (JsonNode fn : metaNode.get("fields")) {
                fields.add(describeFieldNode(fn));
            }
        }
        result.put("fields", fields);
        return result;
    }

    private Map<String, Object> describeFieldNode(JsonNode fn) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", fn.has("name") ? fn.get("name").asText() : "?");
        if (fn.has("type")) info.put("type", fn.get("type").asText());
        info.put("vector", fn.has("vector") && fn.get("vector").asBoolean());
        info.put("i18n", fn.has("i18n") && fn.get("i18n").asBoolean());
        info.put("dynamic", fn.has("dynamic"));
        if (fn.has("dynamic")) {
            Map<String, Object> dynInfo = new LinkedHashMap<>();
            JsonNode dyn = fn.get("dynamic");
            if (dyn.has("class")) dynInfo.put("className", dyn.get("class").asText());
            if (dyn.has("fields") && dyn.get("fields").isArray()) {
                List<String> deps = new ArrayList<>();
                dyn.get("fields").forEach(d -> deps.add(d.asText()));
                dynInfo.put("dependsOn", deps);
            }
            info.put("dynamicInfo", dynInfo);
        }
        if (fn.has("groups") && fn.get("groups").isArray()) {
            List<Map<String, Object>> groups = new ArrayList<>();
            for (JsonNode gn : fn.get("groups")) {
                Map<String, Object> gi = new LinkedHashMap<>();
                if (gn.has("name")) gi.put("name", gn.get("name").asText());
                if (gn.has("method")) gi.put("method", gn.get("method").asText());
                if (gn.has("tags") && gn.get("tags").isArray()) {
                    List<String> tags = new ArrayList<>();
                    gn.get("tags").forEach(t -> tags.add(t.asText()));
                    gi.put("tags", tags);
                }
                groups.add(gi);
            }
            info.put("groups", groups);
        }
        return info;
    }

    // ─── Document Operations ───────────────────────────────────────

    public JVS createDocument(String json) {
        JVS doc = JVS.read(json);
        doc.set("_meta.processed", true);
        doc.set("_meta.timestamp", System.currentTimeMillis());
        if (doc.getType() != null) doc.set("_meta.typeName", doc.getType().getName());
        return doc;
    }

    public JVS mergeDocuments(String baseJson, String overlayJson) {
        JVS base = JVS.read(baseJson);
        JVS overlay = JVS.read(overlayJson);
        JVSMerger.merge(base.getJsonNode(), overlay.getJsonNode());
        return base;
    }

    public Map<String, Object> validateDocument(String json) {
        JVS doc = JVS.read(json);
        Map<String, Object> result = new LinkedHashMap<>();
        Type type = doc.getType();
        if (type == null) {
            result.put("valid", false);
            result.put("error", "No type found. Set 'type' field to e.g. 'core_sysobject'");
            return result;
        }
        result.put("typeName", type.getName());
        List<JVSValidator.Violation> violations = JVSValidator.validate(doc, type);
        result.put("valid", violations.isEmpty());
        if (!violations.isEmpty()) {
            result.put("violations", violations.stream()
                .map(v -> Map.of("level", v.level().name(), "path", v.path(), "message", v.message()))
                .toList());
        }
        return result;
    }

    // ─── Enrichment with Dynamic Fields ────────────────────────────

    public Map<String, Object> enrichDocument(String json, String tags) {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            JVS doc = JVS.read(json);
            result.put("original", doc.getJsonNode());

            Type type = doc.getType();
            if (type == null) {
                result.put("error", "No type found. Set 'type' field (e.g. 'core_sysobject')");
                return result;
            }
            result.put("typeName", type.getName());

            // Use the projection framework to enrich
            String[] tagArray = (tags != null && !tags.isEmpty())
                ? tags.split(",") : new String[]{"basic"};

            JvsEnrichMapper enrichMapper = new JvsEnrichMapper(tagArray);
            JVS enriched = enrichMapper.enrich(doc);

            result.put("enriched", enriched.getJsonNode());
            result.put("tags", tagArray);
        } catch (Exception e) {
            log.error("Enrichment failed", e);
            result.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return result;
    }

    // ─── NLP Stemming ──────────────────────────────────────────────

    public String stemText(String text, String language) {
        SnowballStemmer stemmer = stemmers.getOrDefault(language, stemmers.get("en"));
        String[] words = text.split("\\s+");
        StringBuilder sb = new StringBuilder();
        synchronized (stemmer) {
            for (String word : words) {
                String clean = word.replaceAll("[^\\p{L}]", "").toLowerCase();
                if (!clean.isEmpty()) {
                    if (!sb.isEmpty()) sb.append(" ");
                    stemmer.setCurrent(clean);
                    stemmer.stem();
                    sb.append(stemmer.getCurrent());
                }
            }
        }
        return sb.toString();
    }

    // ─── AI Translation (Ollama) ───────────────────────────────────

    public Map<String, Object> translateDocument(String json, List<String> mlsFields,
                                                  String sourceLang, List<String> targetLangs) {
        Map<String, Object> result = new LinkedHashMap<>();
        long start = System.currentTimeMillis();
        try {
            JVS doc = JVS.read(json);
            result.put("original", doc.getJsonNode());
            result.put("sourceLanguage", sourceLang);
            result.put("targetLanguages", targetLangs);

            // Build MLS field paths (e.g. "title" -> "title.mls")
            List<String> mlsFieldPaths = mlsFields.stream()
                    .map(f -> f.endsWith(".mls") ? f : f + ".mls")
                    .toList();

            com.hitorro.jsontypesystem.JVS2JVSTranslationMapper mapper =
                    com.hitorro.jsontypesystem.JVS2JVSTranslationMapper.builder()
                    .translator(this::callOllamaTranslate)
                    .sourceLanguage(sourceLang)
                    .targetLanguages(targetLangs)
                    .mlsFields(mlsFieldPaths)
                    .build();

            mapper.apply(doc);

            result.put("translated", doc.getJsonNode());
            result.put("success", true);
        } catch (Exception e) {
            log.error("Translation failed", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        result.put("translationTimeMs", System.currentTimeMillis() - start);
        return result;
    }

    String callOllamaTranslate(String text, String sourceLang, String targetLang) {
        try {
            String langName = getLanguageName(targetLang);
            String prompt = "Translate the following text from " + getLanguageName(sourceLang) +
                " to " + langName + ". Return ONLY the translated text, no explanations:\n\n" + text;

            ObjectNode body = JsonNodeFactory.instance.objectNode();
            body.put("model", ollamaModel);
            body.put("prompt", prompt);
            body.put("stream", false);

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ollamaUrl + "/api/generate"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode responseJson = new com.hitorro.util.json.String2JsonMapper().apply(response.body());
            if (responseJson != null && responseJson.has("response")) {
                return responseJson.get("response").asText().trim();
            }
            return "[Translation unavailable - Ollama returned no response]";
        } catch (Exception e) {
            return "[Translation unavailable - " + e.getMessage() + "]";
        }
    }

    public Map<String, Object> getTranslationStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ollamaUrl + "/api/tags"))
                .GET().build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            status.put("available", response.statusCode() == 200);
            status.put("url", ollamaUrl);
            status.put("model", ollamaModel);
        } catch (Exception e) {
            status.put("available", false);
            status.put("error", e.getMessage());
        }
        return status;
    }

    private String getLanguageName(String code) {
        return switch (code) {
            case "en" -> "English"; case "de" -> "German"; case "es" -> "Spanish";
            case "fr" -> "French"; case "it" -> "Italian"; case "pt" -> "Portuguese";
            case "ja" -> "Japanese"; case "zh" -> "Chinese"; case "ko" -> "Korean";
            case "ar" -> "Arabic"; case "ru" -> "Russian"; case "nl" -> "Dutch";
            case "pl" -> "Polish"; case "sv" -> "Swedish"; case "da" -> "Danish";
            default -> code;
        };
    }
}
