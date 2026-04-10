package com.hitorro.jvs.example;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.hamcrest.Matchers.*;

/**
 * Tests for the JVS Example Application.
 *
 * These tests are designed to work WITHOUT HT_BIN set (no type system available).
 * They test endpoints that work independently of the type system, and verify
 * that type-dependent endpoints return proper error responses rather than crashing.
 */
@SpringBootTest
@AutoConfigureMockMvc
@DisplayName("JVS Example Application Tests")
class JvsExampleApplicationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    @DisplayName("Context loads successfully")
    void contextLoads() {
    }

    // ─── Endpoints that always work (no type system needed) ────────

    @Test
    @DisplayName("POST /api/jvs/stem - stems English text")
    void shouldStemText() throws Exception {
        String json = """
            {"text": "The dogs were running happily", "language": "en"}
            """;

        mockMvc.perform(post("/api/jvs/stem")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.original").value("The dogs were running happily"))
            .andExpect(jsonPath("$.stemmed").value(containsString("dog")))
            .andExpect(jsonPath("$.stemmed").value(containsString("run")))
            .andExpect(jsonPath("$.language").value("en"));
    }

    @Test
    @DisplayName("POST /api/jvs/stem - stems German text")
    void shouldStemGermanText() throws Exception {
        String json = """
            {"text": "Die Hunde liefen gluecklich", "language": "de"}
            """;

        mockMvc.perform(post("/api/jvs/stem")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.original").value("Die Hunde liefen gluecklich"))
            .andExpect(jsonPath("$.stemmed").isString())
            .andExpect(jsonPath("$.language").value("de"));
    }

    @Test
    @DisplayName("POST /api/jvs/merge - merges two documents")
    void shouldMergeDocuments() throws Exception {
        String json = """
            {"base": {"name": "original", "shared": "old"}, "overlay": {"shared": "new", "extra": "added"}}
            """;

        mockMvc.perform(post("/api/jvs/merge")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.name").value("original"))
            .andExpect(jsonPath("$.shared").value("new"))
            .andExpect(jsonPath("$.extra").value("added"));
    }

    // ─── Endpoints that return data regardless of type system ──────

    @Test
    @DisplayName("GET /api/jvs/types - returns type list (may be empty)")
    void shouldListTypes() throws Exception {
        mockMvc.perform(get("/api/jvs/types"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$").isArray());
    }

    @Test
    @DisplayName("GET /api/jvs/status - returns status info")
    void shouldReturnStatus() throws Exception {
        mockMvc.perform(get("/api/jvs/status"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.typeSystem").isBoolean())
            .andExpect(jsonPath("$.types").isNumber())
            .andExpect(jsonPath("$.translation").isMap());
    }

    @Test
    @DisplayName("GET /index.html - serves the static UI page")
    void shouldServeStaticPage() throws Exception {
        mockMvc.perform(get("/index.html"))
            .andExpect(status().isOk())
            .andExpect(content().string(containsString("HiTorro JVS Explorer")));
    }

    // ─── Endpoints that degrade gracefully without type system ─────

    @Test
    @DisplayName("POST /api/jvs/documents - creates document with metadata")
    void shouldCreateDocument() throws Exception {
        String json = """
            {"id": {"did": "test-001", "domain": "demo"}, "title": {"mls": [{"lang": "en", "text": "Test Document"}]}}
            """;

        mockMvc.perform(post("/api/jvs/documents")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id.did").value("test-001"))
            .andExpect(jsonPath("$._meta.processed").value(true))
            .andExpect(jsonPath("$._meta.timestamp").isNumber());
    }

    @Test
    @DisplayName("POST /api/jvs/enrich - returns error for non-typed JSON (does not crash)")
    void shouldReturnErrorForEnrichWithoutType() throws Exception {
        String json = """
            {"title": {"mls": [{"lang": "en", "text": "Natural Language Processing"}]}}
            """;

        mockMvc.perform(post("/api/jvs/enrich")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.error").value(containsString("No type found")));
    }

    @Test
    @DisplayName("POST /api/jvs/enrich - structured request returns error for non-typed JSON")
    void shouldReturnErrorForStructuredEnrichWithoutType() throws Exception {
        String json = """
            {"json": "{\\"title\\": \\"hello\\"}", "tags": "basic,segmented"}
            """;

        mockMvc.perform(post("/api/jvs/enrich")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.error").value(containsString("No type found")));
    }

    @Test
    @DisplayName("POST /api/jvs/validate - returns error when no type field present")
    void shouldReturnErrorForValidateWithoutType() throws Exception {
        String json = """
            {"title": {"mls": [{"lang": "en", "text": "No type field"}]}}
            """;

        mockMvc.perform(post("/api/jvs/validate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(json))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.valid").value(false))
            .andExpect(jsonPath("$.error").value(containsString("No type found")));
    }

    @Test
    @DisplayName("GET /api/jvs/types/nonexistent - returns 404 for unknown type")
    void shouldReturn404ForMissingType() throws Exception {
        mockMvc.perform(get("/api/jvs/types/nonexistent_type"))
            .andExpect(status().isNotFound());
    }
}
