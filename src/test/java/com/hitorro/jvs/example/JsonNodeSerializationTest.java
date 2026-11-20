package com.hitorro.jvs.example;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * The HiTorro libraries return Jackson 3 (tools.jackson) JsonNode values, the same Jackson Boot 4 uses
 * for HTTP. Before 4.0.0 they were Jackson 2 nodes, which Jackson 3 would have serialized as a Java bean
 * (nodeType, valueNode, ...), so the app used the Jackson 2 converter. This test proves a controller
 * returning a JsonNode writes plain JSON.
 */
@SpringBootTest
@AutoConfigureMockMvc
@DisplayName("JsonNode responses serialize as plain JSON")
class JsonNodeSerializationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    @DisplayName("POST /api/jvs/merge returns the merged JsonNode as plain JSON")
    void jsonNodeResponseIsPlainJson() throws Exception {
        String request = """
            {"base": {"name": "original", "count": 3, "flag": true, "nested": {"list": [1, 2, "x"]}},
             "overlay": {"shared": "new"}}
            """;
        String expected = """
            {"name": "original", "count": 3, "flag": true, "nested": {"list": [1, 2, "x"]}, "shared": "new"}
            """;

        mockMvc.perform(post("/api/jvs/merge")
                .contentType(MediaType.APPLICATION_JSON)
                .content(request))
            .andExpect(status().isOk())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andExpect(content().json(expected, JsonCompareMode.STRICT))
            .andExpect(content().string(not(containsString("nodeType"))))
            .andExpect(content().string(not(containsString("valueNode"))));
    }
}
