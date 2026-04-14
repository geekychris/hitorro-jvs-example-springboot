package com.hitorro.jvs.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.kvstore.DatabaseConfig;
import com.hitorro.kvstore.KVStore;
import com.hitorro.kvstore.RocksDBStore;
import com.hitorro.kvstore.Result;
import com.hitorro.kvstore.TypedKVStore;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.*;

@Service
public class DocumentStoreService {

    private static final Logger log = LoggerFactory.getLogger(DocumentStoreService.class);

    private TypedKVStore<JsonNode> store;
    private boolean open = false;

    public DocumentStoreService() {
        try {
            String dbPath = new File("data/kvstore").getAbsolutePath();
            DatabaseConfig config = DatabaseConfig.builder(dbPath).build();
            KVStore kvStore = new RocksDBStore(config);
            store = new TypedKVStore<>(kvStore, JsonNode.class);
            open = true;
            log.info("KVStore opened at {}", dbPath);
        } catch (Exception e) {
            log.error("Failed to open KVStore", e);
        }
    }

    @PreDestroy
    public void cleanup() {
        if (store != null) {
            try {
                store.close();
            } catch (Exception e) {
                log.warn("Error closing KVStore", e);
            }
        }
    }

    public boolean isOpen() {
        return open;
    }

    /**
     * Build a key from a JVS document's id fields.
     */
    public String buildKey(JsonNode doc) {
        JsonNode id = doc.get("id");
        if (id != null && id.has("domain") && id.has("did")) {
            return id.get("domain").asText() + "/" + id.get("did").asText();
        }
        return null;
    }

    public Result<Void> put(String key, JsonNode doc) {
        if (!open) return Result.failure(new IllegalStateException("KVStore not open"));
        return store.put(key, doc);
    }

    public Result<JsonNode> get(String key) {
        if (!open) return Result.failure(new IllegalStateException("KVStore not open"));
        return store.get(key);
    }

    /**
     * Store all documents, keyed by id.domain/id.did.
     * Returns the number stored.
     */
    public int storeAll(List<JsonNode> docs) {
        int count = 0;
        for (JsonNode doc : docs) {
            String key = buildKey(doc);
            if (key != null) {
                Result<Void> r = store.put(key, doc);
                if (r.isSuccess()) count++;
            }
        }
        return count;
    }

    public Map<String, Object> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("open", open);
        return status;
    }
}
