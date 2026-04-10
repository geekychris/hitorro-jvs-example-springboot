package com.hitorro.jvs.example;

import com.hitorro.jsontypesystem.BaseT;
import com.hitorro.jsontypesystem.Group;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jsontypesystem.executors.BaseProjectionFactoryMapper;
import com.hitorro.jsontypesystem.executors.EnrichAction;
import com.hitorro.jsontypesystem.executors.EnrichExecutionBuilderMapper;
import com.hitorro.jsontypesystem.executors.ExecutionBuilder;
import com.hitorro.jsontypesystem.executors.ProjectionContext;
import com.hitorro.util.core.events.cache.HashCache;
import com.hitorro.util.core.map.MapUtil;
import com.hitorro.util.json.keys.propaccess.PropaccessError;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Enrichment mapper that uses the JVS projection framework to apply dynamic field mappers
 * (sentence segmentation, NER, POS tagging, hashing, etc.) based on enrichment tags.
 *
 * This is a self-contained version of the enrichment logic from hitorro-objretrieval's
 * JVS2JVSEnrichMapper, so the example app doesn't need the full objretrieval dependency.
 */
public class JvsEnrichMapper {

    private final HashCache<Type, ExecutionBuilder> cache;

    /**
     * Create an enrichment mapper for the given tags.
     * @param tags enrichment tags like "segmented", "ner", "pos", "hash", "parsed", "answers"
     *             If null or empty, only "basic" tagged fields are enriched.
     */
    public JvsEnrichMapper(String... tags) {
        EnrichExecutionBuilderMapper mapper = new EnrichExecutionBuilderMapper();
        if (tags == null || tags.length == 0) {
            String[] basic = {"basic"};
            mapper.setPredicate(mapper.getPredicate().and(tagPredicate(basic).or(nullTagPredicate())));
        } else {
            mapper.setPredicate(mapper.getPredicate().and(tagPredicate(tags).or(nullTagPredicate())));
        }
        String cacheKey = buildCacheKey(tags);
        cache = Type.getExecBuilderCache(cacheKey, mapper);
    }

    /**
     * Enrich a JVS document by executing dynamic field mappers.
     * The document must have a "type" field that resolves to a known type definition.
     */
    public JVS enrich(JVS doc) {
        Type type = doc.getType();
        if (type == null) return doc;

        ProjectionContext pc = new ProjectionContext();
        pc.source = doc;
        pc.target = new JVS();
        try {
            ExecutionBuilder builder = cache.get(type);
            if (builder == null) {
                org.slf4j.LoggerFactory.getLogger(JvsEnrichMapper.class).warn("No execution builder for type: {}", type.getName());
            } else if (builder.getCurrentNode() == null) {
                org.slf4j.LoggerFactory.getLogger(JvsEnrichMapper.class).warn("Execution builder has no nodes for type: {}", type.getName());
            } else {
                org.slf4j.LoggerFactory.getLogger(JvsEnrichMapper.class).info("Enriching type {} with {} actions", type.getName(), builder.getCurrentNode().getChildren() != null ? builder.getCurrentNode().getChildren().size() : 0);
                builder.getCurrentNode().project(pc);
            }
        } catch (PropaccessError e) {
            org.slf4j.LoggerFactory.getLogger(JvsEnrichMapper.class).error("Enrichment failed for type {}: {}", type.getName(), e.getMessage());
        }
        return pc.source;
    }

    private String buildCacheKey(String[] tags) {
        if (tags == null || tags.length == 0) return "enrich:basic";
        StringBuilder sb = new StringBuilder("enrich:");
        for (int i = 0; i < tags.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(tags[i]);
        }
        return sb.toString();
    }

    private static Predicate<BaseT> tagPredicate(String[] tags) {
        Set<String> tagSet = MapUtil.getSet(tags);
        return b -> {
            if (b instanceof Group group) {
                List<String> groupTags = group.getTags();
                if (groupTags != null) {
                    for (String t : groupTags) {
                        if (tagSet.contains(t)) return true;
                    }
                }
            }
            return false;
        };
    }

    private static Predicate<BaseT> nullTagPredicate() {
        return b -> {
            if (b instanceof Group group) {
                List<String> tags = group.getTags();
                return tags == null || tags.isEmpty();
            }
            return false;
        };
    }
}
