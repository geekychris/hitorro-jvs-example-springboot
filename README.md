# HiTorro JVS Spring Boot Example

A Spring Boot application demonstrating the **hitorro-jsontypesystem** library: type system, NLP enrichment, NER (Named Entity Recognition), translation, and NDJSON document processing pipelines.

## Quick Start

```bash
# Build (from parent hitorro directory)
cd hitorro-jvs-example-springboot
mvn clean package -DskipTests

# Run
java -jar target/hitorro-jsontypesystem-example-springboot-1.0.0.jar

# Or with Maven
mvn spring-boot:run
```

Open [http://localhost:8080](http://localhost:8080) to access the interactive UI.

## Features

- **Type Explorer** -- Browse and inspect JVS type definitions
- **Enrich & NLP** -- Enrich documents with stemming, segmentation, POS tagging, NER
- **Translation** -- Translate MLS fields via Ollama LLM
- **Documents** -- Create, validate, and merge typed JVS documents
- **NDJSON Pipeline** -- Process document datasets through enrichment and translation pipelines

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

- Java 21+
- Maven 3.8+
- Ollama running locally (optional, for translation features)
- Parent `hitorro` project with `config/` directory (for type definitions and NLP data)

## Configuration

`application.yml`:

```yaml
server:
  port: 8080
```

Environment variables:
- `HT_BIN` / `HT_HOME` -- Path to hitorro root (auto-detected from parent directory)
- `OLLAMA_URL` -- Ollama API URL (default: `http://localhost:11434`)
- `OLLAMA_MODEL` -- Ollama model name (default: `llama3.2`)
