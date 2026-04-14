# HiTorro JVS Spring Boot Example

A Spring Boot application demonstrating the **hitorro-jsontypesystem** library with integrated Lucene search, RocksDB document storage, multi-language NLP enrichment, NER (Named Entity Recognition via MaxEnt and ONNX transformer models), and NDJSON document processing pipelines.

## Quick Start

```bash
# 1. Build (from parent hitorro directory)
cd hitorro-jvs-example-springboot
mvn clean package -DskipTests

# 2. (Optional) Download ONNX NER model for multilingual NER support
#    Requires Python 3.8+ — only needed once
cd .. && ./download-onnx-models.sh && cd hitorro-jvs-example-springboot

# 3. Run (HT_BIN tells the app where to find config/ and data/)
mvn spring-boot:run -DHT_BIN=/path/to/hitorro

# Or run the JAR directly
java -DHT_BIN=/path/to/hitorro -jar target/hitorro-jsontypesystem-example-springboot-1.0.0.jar
```

Open [http://localhost:8080](http://localhost:8080) to access the interactive UI.

## Features

- **Type Explorer** -- Browse and inspect JVS type definitions, field hierarchies, and dynamic field chains
- **Enrich & NLP** -- Enrich documents with stemming, segmentation, POS tagging, NER (MaxEnt + ONNX transformer fallback)
- **Translation** -- Translate MLS fields to multiple languages via Ollama LLM
- **Search & Index** -- Lucene full-text search with type-aware field mapping, faceting, and multi-language support
- **KV Store** -- RocksDB persistent document storage with option to fetch full documents from the store instead of the index
- **Documents** -- Create, validate, and merge typed JVS documents
- **NDJSON Pipeline** -- Process document datasets through enrichment and translation pipelines
- **Index Viewer** -- Inspect Lucene index internals (stats, fields, stored documents, terms)

## Search, Indexing & Document Storage

The app demonstrates a complete document processing pipeline: translate, enrich, index, and search with both Lucene and RocksDB KV store backends.

### Index & Enrich Pipeline

The indexing pipeline processes documents through a stream chain:

```java
List<JVS> processedDocs = pipelineService.readDataset().stream()
        .map(JVS::new)          // Load JSON → JVS
        .map(translate)          // Add multi-language MLS entries (Ollama)
        .map(enrich)             // Compute NLP dynamic fields on all languages
        .collect(Collectors.toList());
```

**Pipeline stages:**
1. **Load** — Read NDJSON dataset into JVS documents
2. **Translate** (optional) — Add multi-language MLS entries via Ollama (e.g., German, Spanish, French). Translation adds new `{lang, text}` entries to the MLS arrays.
3. **Enrich** (optional) — Compute NLP dynamic fields (segmentation, NER, POS) across ALL language entries, including translated ones. Enrichment runs after translation so NLP fields are computed for every language.
4. **Index** — Store enriched documents in Lucene (type-aware field projection creates language-specific fields like `title.mls.text_en_s`, `title.mls.text_de_s`) and RocksDB KV store.

### Multi-Language Search

The type system's `iso-language-seeker` and `i18n: true` field markers automatically create language-specific Lucene fields. After indexing with translations:

- `title.mls.text_en_s` — English title (analyzed with `EnglishAnalyzer`)
- `title.mls.text_de_s` — German title (analyzed with `GermanAnalyzer`)
- `body.mls.segmented_ner_en_m` — English NER markup
- `body.mls.segmented_ner_de_m` — German NER markup (via ONNX transformer)

Use the language selector in the Search tab to switch between languages for query parsing.

### Lucene `_source` Field

By default, the full document JSON is stored as a `_source` field in the Lucene index for faithful reconstruction in search results. This can be disabled via `IndexConfig.builder().storeSource(false)` when using the KV store for full document retrieval (saves index storage).

### KV Store Integration

Documents are stored in RocksDB at `data/kvstore/` keyed by `{domain}/{did}`. The search UI provides a toggle to fetch results from the KV store instead of the Lucene index:

- **Source: Index** (default) — Documents reconstructed from Lucene `_source` field
- **Source: KV Store** — Full documents fetched from RocksDB by document ID (extracted from Lucene hits)

This demonstrates a common pattern: use the index for search/ranking, use an external store for full document retrieval.

### Search API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/jvs/search/index` | POST | Index dataset with optional enrichment and translation |
| `/api/jvs/search` | GET | Search with query, facets, language, and KV store toggle |
| `/api/jvs/search/doc/{domain}/{did}` | GET | Fetch single document from KV store |
| `/api/jvs/search/fields/{typeName}` | GET | Get indexed field names for a type |
| `/api/jvs/search/status` | GET | Index and KV store status |

**Index request body:**
```json
{
  "enrichTags": "basic,segmented,ner",
  "targetLangs": "de,es"
}
```

**Search query parameters:**
- `q` — Lucene query string (e.g., `title.mls.clean:climate`, `*:*`)
- `lang` — Language for i18n field resolution (default: `en`)
- `useKvStore` — `true` to fetch full docs from RocksDB instead of index (default: `false`)
- `facets` — Facet dimensions (default: `department,classification`)
- `offset`, `limit` — Pagination

### Index Viewer API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/jvs/viewer/stats` | GET | Index statistics (doc count, fields, segments) |
| `/api/jvs/viewer/fields` | GET | List all indexed fields with cardinality |
| `/api/jvs/viewer/documents` | GET | Browse stored documents |
| `/api/jvs/viewer/terms/{field}` | GET | Browse terms in a field |

## NDJSON Document Dataset

The `data/demo-documents.ndjson` file contains 10 documents of type `demo_document`, each with:

- **Multi-line title and body** in English MLS format
- **Rich named entities**: people (Sarah Mitchell, Dr. Maria Santos, Tim Cook), organizations (Acme Corporation, NASA, Morgan Stanley, Apple Inc.), locations (San Francisco, London, Paris, Chicago), dates, and monetary amounts
- **Realistic metadata**: department, classification, keywords, file info

### Sample Document Structure

```json
{
  "ht_type": "demo_document",
  "id": {"domain": "legal", "did": "doc-001"},
  "title": {"mls": [{"lang": "en", "text": "Merger Agreement Between Acme Corporation and GlobalTech Industries.\nApproved by the Board of Directors on March 15, 2024."}]},
  "body": {"mls": [{"lang": "en", "text": "This merger agreement is entered into by Acme Corporation..."}]},
  "author": "Sarah Mitchell",
  "department": "Legal",
  "keywords": ["merger", "acquisition", "regulatory", "corporate"],
  "classification": "confidential"
}
```

## NDJSON Pipeline Examples

The pipeline demonstrates two equivalent approaches to processing NDJSON documents:

### Hitorro Iterator / Mapper / Sink

Uses `JSONIterator` to stream-parse NDJSON, `AbstractIterator.map()` for transformation, `JvsEnrichMapper` for NLP enrichment, and `JsonSink` to write results back to NDJSON:

```java
try (JSONIterator jsonIter = new JSONIterator(reader)) {
    JsonSink sink = new JsonSink(outputStream);
    sink.start();

    AbstractIterator<JsonNode> pipeline = jsonIter
        .map(json -> {
            JVS jvs = new JVS(json);
            return enrichMapper.enrich(jvs).getJsonNode();
        });

    while (pipeline.hasNext()) {
        sink.add(pipeline.next());
    }
    sink.stop();
}
```

### Java Streams

Uses standard Java Streams API with the same transformation logic:

```java
List<JsonNode> results = inputDocs.stream()
    .map(json -> new JVS(json))
    .map(jvs -> enrichMapper.enrich(jvs))
    .map(JVS::getJsonNode)
    .collect(Collectors.toList());
```

### Iterator <-> Stream Bridges

Hitorro iterators bridge seamlessly to/from Java Streams:

```java
// Iterator -> Stream: use toStream()
try (Stream<JsonNode> stream = jsonIterator.toStream()) {
    List<JsonNode> results = stream
        .map(json -> new JVS(json))
        .map(jvs -> enrichMapper.enrich(jvs))
        .map(JVS::getJsonNode)
        .collect(Collectors.toList());
}

// Stream -> Iterator -> Sink: use fromStream()
Stream<JsonNode> enrichedStream = docs.stream()
    .map(json -> enrichMapper.enrich(new JVS(json)).getJsonNode());

AbstractIterator.fromStream(enrichedStream).sink(new JsonSink(outputStream));
```

### Translation Pipeline

Translates MLS fields in title and body to target languages via Ollama:

```java
AbstractIterator<JsonNode> pipeline = jsonIter
    .map(json -> {
        JVS jvs = new JVS(json);
        translateJvs(jvs, List.of("title", "body"), "en", List.of("de", "es"));
        return jvs.getJsonNode();
    });
```

### Combined Pipeline (Enrich + Translate)

Chains enrichment and translation in a single pass:

```java
AbstractIterator<JsonNode> pipeline = jsonIter
    .map(json -> {
        JVS jvs = new JVS(json);
        JVS enriched = enrichMapper.enrich(jvs);
        translateJvs(enriched, fields, "en", List.of("de", "es"));
        return enriched.getJsonNode();
    });
```

## Pipeline API Endpoints

All pipeline endpoints are under `/api/jvs/pipeline/`:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/pipeline/dataset` | GET | List all documents in the NDJSON dataset |
| `/pipeline/dataset/count` | GET | Get document count and file path |
| `/pipeline/enrich/iterator` | POST | Enrich via hitorro iterator pipeline |
| `/pipeline/enrich/stream` | POST | Enrich via Java Streams pipeline |
| `/pipeline/translate/iterator` | POST | Translate via hitorro iterator pipeline |
| `/pipeline/translate/stream` | POST | Translate via Java Streams pipeline |
| `/pipeline/combined/iterator` | POST | Enrich + translate via iterator pipeline |
| `/pipeline/combined/stream` | POST | Enrich + translate via Java Streams pipeline |
| `/pipeline/bridge/iterator-to-stream` | POST | Demo: JSONIterator.toStream() bridge |
| `/pipeline/bridge/stream-to-sink` | POST | Demo: AbstractIterator.fromStream() -> JsonSink |

### Request Body (POST endpoints)

```json
{
  "tags": "basic,segmented,ner",
  "sourceLanguage": "en",
  "targetLanguages": ["de", "es", "fr"]
}
```

### Enrichment Tags

| Tag | NLP Feature |
|-----|-------------|
| `basic` | Stemming, normalization |
| `segmented` | Sentence segmentation, clean text |
| `ner` | Named Entity Recognition (person, location, organization, date, money) |
| `pos` | Part-of-speech tagging |
| `hash` | Normalized text hashing |
| `parsed` | Dependency parsing |

### Output Files

Pipeline results are written to the `data/` directory:

- `demo-documents-enriched.ndjson` -- Iterator enrichment output
- `demo-documents-enriched-stream.ndjson` -- Stream enrichment output
- `demo-documents-translated.ndjson` -- Iterator translation output
- `demo-documents-translated-stream.ndjson` -- Stream translation output
- `demo-documents-combined.ndjson` -- Iterator combined output
- `demo-documents-combined-stream.ndjson` -- Stream combined output
- `demo-documents-stream-to-sink.ndjson` -- Bridge example output

## Other API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/jvs/types` | GET | List all available types |
| `/api/jvs/types/{name}` | GET | Get type definition |
| `/api/jvs/status` | GET | System status |
| `/api/jvs/documents` | POST | Create a JVS document |
| `/api/jvs/merge` | POST | Merge two JVS documents |
| `/api/jvs/validate` | POST | Validate against type schema |
| `/api/jvs/enrich` | POST | Enrich a single document |
| `/api/jvs/stem` | POST | Stem text (Snowball) |
| `/api/jvs/translate` | POST | Translate MLS fields (Ollama) |

## Prerequisites

- **Java 21+**
- **Maven 3.8+**
- **Parent `hitorro` project** with `config/` directory (type definitions) and `data/` directory (NLP models)
- **Ollama** running locally (optional, for translation features)
- **Python 3.8+** (optional, one-time setup for ONNX NER model export)

### NLP Model Setup

The app uses OpenNLP models from `data/opennlpmodels1.5/` for sentence detection, tokenization, POS tagging, and NER. These are included in the repository.

For multilingual NER via ONNX transformer (German, French, Italian, Portuguese, etc.):

```bash
# One-time setup (requires Python 3.8+ and pip)
cd /path/to/hitorro
./download-onnx-models.sh
```

This exports `Davlan/xlm-roberta-base-ner-hrl` (~1GB ONNX model) to `data/opennlpmodels-onnx/ner-multilingual/`. After this, NER enrichment works for all supported languages automatically.

## Configuration

`application.yml`:

```yaml
server:
  port: 8080
```

**Environment variables / system properties:**

| Variable | Default | Description |
|----------|---------|-------------|
| `HT_BIN` / `HT_HOME` | auto-detected from parent dir | Path to hitorro root (contains `config/` and `data/`) |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama API URL for translation |
| `OLLAMA_MODEL` | `llama3.2` | Ollama model name |

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    JVS Example App (Spring Boot)             │
├──────────────────────────────────────────────────────────────┤
│  Controllers:  JvsController, SearchController,              │
│                NdjsonPipelineController, LuceneViewerController│
├──────────────────────────────────────────────────────────────┤
│  Services:     JvsService (type system + NLP + translation)  │
│                SearchService (index pipeline + search)       │
│                NdjsonPipelineService (batch processing)       │
│                DocumentStoreService (RocksDB KV)             │
├──────────────────────────────────────────────────────────────┤
│  Libraries:    hitorro-jsontypesystem (JVS + types + NLP)    │
│                hitorro-index (Lucene indexing + search)       │
│                hitorro-kvstore (RocksDB key-value store)      │
└──────────────────────────────────────────────────────────────┘
```

### Key Classes

| Class | Purpose |
|-------|---------|
| `SearchService` | Stream pipeline: load → translate → enrich → index + KV store. Handles search with index/KV source toggle. |
| `JvsEnrichMapper` | Wraps `EnrichExecutionBuilderMapper` for NLP enrichment with tag-based filtering |
| `DocumentStoreService` | RocksDB wrapper for persistent document storage keyed by `{domain}/{did}` |
| `NdjsonPipelineService` | NDJSON processing demonstrating iterator, stream, and bridge patterns |

### Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| Spring Boot | 3.2.2 | Web framework |
| hitorro-jsontypesystem | 3.0.1 | JVS type system + NLP enrichment |
| hitorro-index | 3.0.0 | Lucene full-text search with type-aware field mapping |
| hitorro-kvstore | 3.0.1 | RocksDB persistent key-value store |
| hitorro-luceneviewer | 3.0.0 | Lucene index inspection tools |
