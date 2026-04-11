package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.JVS2JVSTranslationMapper;
import com.hitorro.util.json.keys.propaccess.Propaccess;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("MLS Translation")
class MlsTranslationTest {

    // ─── Low-level JVS.append tests ───────────────────────────────

    @Nested
    @DisplayName("JVS.append for MLS elements")
    class AppendTests {

        @Test
        @DisplayName("Appending a single translation produces a clean MLS element with lang and text")
        void shouldAppendSingleTranslation() {
            JVS doc = JVS.read("""
                {"title": {"mls": [{"lang": "en", "text": "Hello World"}]}}
                """);

            ObjectNode mlsElem = JsonNodeFactory.instance.objectNode();
            mlsElem.put("lang", "de");
            mlsElem.put("text", "Hallo Welt");
            doc.append(new Propaccess("title.mls"), mlsElem);

            JsonNode mls = doc.get("title.mls");
            assertThat(mls.isArray()).isTrue();
            assertThat(mls.size()).isEqualTo(2);

            assertThat(mls.get(0).get("lang").asText()).isEqualTo("en");
            assertThat(mls.get(0).get("text").asText()).isEqualTo("Hello World");

            JsonNode deElem = mls.get(1);
            assertThat(deElem.isObject()).isTrue();
            assertThat(deElem.get("lang").asText()).isEqualTo("de");
            assertThat(deElem.get("text").asText()).isEqualTo("Hallo Welt");
            assertThat(deElem.size()).isEqualTo(2);
        }

        @Test
        @DisplayName("Appending preserves all fields on the original element")
        void shouldPreserveOriginalElementFields() {
            JVS doc = JVS.read("""
                {"title": {"mls": [{"lang": "en", "text": "Hello", "clean": "hello"}]}}
                """);

            ObjectNode mlsElem = JsonNodeFactory.instance.objectNode();
            mlsElem.put("lang", "fr");
            mlsElem.put("text", "Bonjour");
            doc.append(new Propaccess("title.mls"), mlsElem);

            JsonNode mls = doc.get("title.mls");
            assertThat(mls.get(0).get("clean").asText()).isEqualTo("hello");
            assertThat(mls.get(1).get("lang").asText()).isEqualTo("fr");
            assertThat(mls.get(1).get("text").asText()).isEqualTo("Bonjour");
        }

        @Test
        @DisplayName("Appending to an empty MLS array works correctly")
        void shouldAppendToEmptyMlsArray() {
            JVS doc = JVS.read("""
                {"title": {"mls": []}}
                """);

            ObjectNode mlsElem = JsonNodeFactory.instance.objectNode();
            mlsElem.put("lang", "en");
            mlsElem.put("text", "First entry");
            doc.append(new Propaccess("title.mls"), mlsElem);

            JsonNode mls = doc.get("title.mls");
            assertThat(mls.size()).isEqualTo(1);
            assertThat(mls.get(0).get("lang").asText()).isEqualTo("en");
            assertThat(mls.get(0).get("text").asText()).isEqualTo("First entry");
        }

        @Test
        @DisplayName("Appended entries are readable via Propaccess index")
        void shouldBeReadableViaPropaccessAfterAppend() {
            JVS doc = JVS.read("""
                {"title": {"mls": [{"lang": "en", "text": "Hello"}]}}
                """);

            ObjectNode mlsElem = JsonNodeFactory.instance.objectNode();
            mlsElem.put("lang", "ja");
            mlsElem.put("text", "Konnichiwa");
            doc.append(new Propaccess("title.mls"), mlsElem);

            assertThat(doc.getString("title.mls[1].lang")).isEqualTo("ja");
            assertThat(doc.getString("title.mls[1].text")).isEqualTo("Konnichiwa");
            assertThat(doc.getString("title.mls[0].lang")).isEqualTo("en");
        }
    }

    // ─── JVS2JVSTranslationMapper tests ───────────────────────────────

    @Nested
    @DisplayName("JVS2JVSTranslationMapper")
    class MapperTests {

        private static final Map<String, String> FAKE_TRANSLATIONS = Map.of(
                "en->de", "Hallo Welt",
                "en->es", "Hola Mundo",
                "en->fr", "Bonjour le monde"
        );

        private String fakeTranslate(String text, String src, String tgt) {
            String key = src + "->" + tgt;
            return FAKE_TRANSLATIONS.getOrDefault(key, "[" + tgt + "] " + text);
        }

        @Test
        @DisplayName("Mapper translates title and body to multiple languages with clean MLS entries")
        void shouldTranslateMultipleFieldsAndLanguages() {
            JVS doc = JVS.read("""
                {
                  "title": {"mls": [{"lang": "en", "text": "Hello World"}]},
                  "body": {"mls": [{"lang": "en", "text": "Hello World"}]}
                }
                """);

            JVS2JVSTranslationMapper mapper = JVS2JVSTranslationMapper.builder()
                    .translator(this::fakeTranslate)
                    .sourceLanguage("en")
                    .targetLanguages("de", "es")
                    .mlsFields("title.mls", "body.mls")
                    .build();

            JVS result = mapper.apply(doc);
            assertThat(result).isSameAs(doc);

            for (String field : new String[]{"title", "body"}) {
                JsonNode mls = result.get(field + ".mls");
                assertThat(mls.size()).as(field + ".mls size").isEqualTo(3);

                for (int i = 0; i < mls.size(); i++) {
                    JsonNode elem = mls.get(i);
                    assertThat(elem.isObject()).as("%s.mls[%d] is object", field, i).isTrue();
                    assertThat(elem.get("lang").isTextual()).as("%s.mls[%d].lang is string", field, i).isTrue();
                    assertThat(elem.get("text").isTextual()).as("%s.mls[%d].text is string", field, i).isTrue();
                }

                assertThat(mls.get(0).get("lang").asText()).isEqualTo("en");
                assertThat(mls.get(1).get("lang").asText()).isEqualTo("de");
                assertThat(mls.get(1).get("text").asText()).isEqualTo("Hallo Welt");
                assertThat(mls.get(2).get("lang").asText()).isEqualTo("es");
                assertThat(mls.get(2).get("text").asText()).isEqualTo("Hola Mundo");
            }
        }

        @Test
        @DisplayName("Mapper skips fields that have no source language text")
        void shouldSkipFieldsWithoutSourceText() {
            JVS doc = JVS.read("""
                {
                  "title": {"mls": [{"lang": "de", "text": "Nur Deutsch"}]},
                  "body": {"mls": [{"lang": "en", "text": "Has English"}]}
                }
                """);

            JVS2JVSTranslationMapper mapper = JVS2JVSTranslationMapper.builder()
                    .translator(this::fakeTranslate)
                    .sourceLanguage("en")
                    .targetLanguages("fr")
                    .mlsFields("title.mls", "body.mls")
                    .build();

            mapper.apply(doc);

            // title should be unchanged (no English source)
            assertThat(doc.get("title.mls").size()).isEqualTo(1);
            assertThat(doc.get("title.mls").get(0).get("lang").asText()).isEqualTo("de");

            // body should have the French translation added
            assertThat(doc.get("body.mls").size()).isEqualTo(2);
            assertThat(doc.get("body.mls").get(1).get("lang").asText()).isEqualTo("fr");
        }

        @Test
        @DisplayName("Mapper skips target language that already exists")
        void shouldSkipExistingTranslations() {
            JVS doc = JVS.read("""
                {
                  "title": {"mls": [
                    {"lang": "en", "text": "Hello"},
                    {"lang": "de", "text": "Existing German"}
                  ]}
                }
                """);

            AtomicInteger callCount = new AtomicInteger(0);
            JVS2JVSTranslationMapper mapper = JVS2JVSTranslationMapper.builder()
                    .translator((text, src, tgt) -> {
                        callCount.incrementAndGet();
                        return fakeTranslate(text, src, tgt);
                    })
                    .sourceLanguage("en")
                    .targetLanguages("de", "es")
                    .mlsFields("title.mls")
                    .build();

            mapper.apply(doc);

            // Should only call translator for "es", not "de" (already exists)
            assertThat(callCount.get()).isEqualTo(1);
            assertThat(doc.get("title.mls").size()).isEqualTo(3);

            // Original German text preserved, not overwritten
            assertThat(doc.get("title.mls").get(1).get("text").asText()).isEqualTo("Existing German");
            assertThat(doc.get("title.mls").get(2).get("lang").asText()).isEqualTo("es");
        }

        @Test
        @DisplayName("Mapper skips source language in target list")
        void shouldSkipSourceLanguageInTargets() {
            JVS doc = JVS.read("""
                {"title": {"mls": [{"lang": "en", "text": "Hello"}]}}
                """);

            AtomicInteger callCount = new AtomicInteger(0);
            JVS2JVSTranslationMapper mapper = JVS2JVSTranslationMapper.builder()
                    .translator((text, src, tgt) -> {
                        callCount.incrementAndGet();
                        return "[" + tgt + "]";
                    })
                    .sourceLanguage("en")
                    .targetLanguages("en", "de")
                    .mlsFields("title.mls")
                    .build();

            mapper.apply(doc);

            assertThat(callCount.get()).isEqualTo(1); // only "de" translated
            assertThat(doc.get("title.mls").size()).isEqualTo(2);
        }

        @Test
        @DisplayName("Mapper handles null JVS gracefully")
        void shouldHandleNullJvs() {
            JVS2JVSTranslationMapper mapper = JVS2JVSTranslationMapper.builder()
                    .translator(this::fakeTranslate)
                    .sourceLanguage("en")
                    .targetLanguages("de")
                    .mlsFields("title.mls")
                    .build();

            assertThat(mapper.apply(null)).isNull();
        }

        @Test
        @DisplayName("Mapper continues on error when skipOnError is true")
        void shouldContinueOnErrorByDefault() {
            JVS doc = JVS.read("""
                {
                  "title": {"mls": [{"lang": "en", "text": "Hello"}]},
                  "body": {"mls": [{"lang": "en", "text": "World"}]}
                }
                """);

            JVS2JVSTranslationMapper mapper = JVS2JVSTranslationMapper.builder()
                    .translator((text, src, tgt) -> {
                        if (text.equals("Hello")) throw new RuntimeException("Simulated failure");
                        return "[" + tgt + "] " + text;
                    })
                    .sourceLanguage("en")
                    .targetLanguages("de")
                    .mlsFields("title.mls", "body.mls")
                    .skipOnError(true)
                    .build();

            mapper.apply(doc);

            // title translation failed, body should still succeed
            assertThat(doc.get("title.mls").size()).isEqualTo(1); // no translation added
            assertThat(doc.get("body.mls").size()).isEqualTo(2);  // translation added
            assertThat(doc.get("body.mls").get(1).get("lang").asText()).isEqualTo("de");
        }

        @Test
        @DisplayName("Builder validates required fields")
        void shouldValidateBuilderRequiredFields() {
            assertThatThrownBy(() -> JVS2JVSTranslationMapper.builder()
                    .targetLanguages("de").mlsFields("title.mls").build())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Translator");

            assertThatThrownBy(() -> JVS2JVSTranslationMapper.builder()
                    .translator(this::fakeTranslate).mlsFields("title.mls").build())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("target language");

            assertThatThrownBy(() -> JVS2JVSTranslationMapper.builder()
                    .translator(this::fakeTranslate).targetLanguages("de").build())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("MLS field");
        }

        @Test
        @DisplayName("Mapper works as a Function in stream pipelines")
        void shouldWorkInStreamPipeline() {
            JVS2JVSTranslationMapper mapper = JVS2JVSTranslationMapper.builder()
                    .translator(this::fakeTranslate)
                    .sourceLanguage("en")
                    .targetLanguages("de", "fr")
                    .mlsFields("title.mls")
                    .build();

            JVS[] docs = {
                    JVS.read("{\"title\": {\"mls\": [{\"lang\": \"en\", \"text\": \"Hello World\"}]}}"),
                    JVS.read("{\"title\": {\"mls\": [{\"lang\": \"en\", \"text\": \"Hello World\"}]}}")
            };

            long translatedCount = java.util.Arrays.stream(docs)
                    .map(mapper)
                    .filter(jvs -> jvs.get("title.mls").size() == 3)
                    .count();

            assertThat(translatedCount).isEqualTo(2);
        }
    }
}
