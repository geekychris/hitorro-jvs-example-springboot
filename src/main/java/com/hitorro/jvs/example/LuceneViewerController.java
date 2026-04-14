package com.hitorro.jvs.example;

import com.hitorro.luceneviewer.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/jvs/viewer")
@CrossOrigin(origins = "*")
public class LuceneViewerController {

    private final SearchService searchService;

    public LuceneViewerController(SearchService searchService) {
        this.searchService = searchService;
    }

    private DirectoryReader getReader() throws Exception {
        if (!searchService.isIndexed()) {
            throw new IllegalStateException("No index available. Index documents first.");
        }
        var handle = searchService.getIndexManager().getIndex("documents");
        if (handle == null) throw new IllegalStateException("Index 'documents' not found.");
        // Get the Directory from the writer's underlying index
        return DirectoryReader.open(handle.getWriter().getIndexWriter());
    }

    @GetMapping("/stats")
    public ResponseEntity<?> stats() {
        try {
            DirectoryReader reader = getReader();
            LuceneIndexStats stats = LuceneViewer.stats(reader);
            reader.close();
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/fields")
    public ResponseEntity<?> listFields() {
        try {
            DirectoryReader reader = getReader();
            List<LuceneFieldSummary> fields = LuceneViewer.listFields(reader);
            reader.close();
            return ResponseEntity.ok(fields);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/documents")
    public ResponseEntity<?> listDocuments(
            @RequestParam(defaultValue = "0") int start,
            @RequestParam(defaultValue = "10") int limit) {
        try {
            DirectoryReader reader = getReader();
            List<LuceneStoredDocument> docs = LuceneViewer.listDocuments(reader, start, limit, true, false);
            reader.close();
            return ResponseEntity.ok(docs);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/documents/{docId}")
    public ResponseEntity<?> getDocument(@PathVariable int docId) {
        try {
            DirectoryReader reader = getReader();
            LuceneStoredDocument doc = LuceneViewer.getStoredDocument(reader, docId);
            reader.close();
            return ResponseEntity.ok(doc);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/terms/{field}")
    public ResponseEntity<?> listTerms(
            @PathVariable String field,
            @RequestParam(required = false) String after,
            @RequestParam(defaultValue = "50") int limit) {
        try {
            DirectoryReader reader = getReader();
            LuceneTermsPage page = LuceneViewer.listTerms(reader, field, after, limit);
            reader.close();
            return ResponseEntity.ok(page);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/search")
    public ResponseEntity<?> search(
            @RequestParam String q,
            @RequestParam(defaultValue = "content") String field,
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(required = false) String pageToken) {
        try {
            DirectoryReader reader = getReader();
            IndexSearcher searcher = new IndexSearcher(reader);
            LuceneSearchPage page = LuceneViewer.search(searcher, q, field, limit, pageToken, true);
            reader.close();
            return ResponseEntity.ok(page);
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }
}
