package com.hitorro.jvs.example;

import com.hitorro.jsontypesystem.BaseT;
import com.hitorro.jsontypesystem.Group;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jsontypesystem.executors.EnrichExecutionBuilderMapper;
import com.hitorro.jsontypesystem.executors.ExecutionBuilder;
import com.hitorro.jsontypesystem.executors.ProjectionContext;
import com.hitorro.util.core.ArrayUtil;
import com.hitorro.util.core.ListUtil;
import com.hitorro.util.core.events.cache.HashCache;
import com.hitorro.util.core.map.MapUtil;
import com.hitorro.util.json.keys.propaccess.PropaccessError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Enrichment mapper that replicates the exact logic from hitorro-objretrieval's
 * BaseProjectionMapper + JVS2JVSEnrichMapper, so the JVS example app doesn't
 * need the full objretrieval dependency (which brings in Hadoop/Solr/etc).
 */
public class JvsEnrichMapper {

    private static final Logger log = LoggerFactory.getLogger(JvsEnrichMapper.class);
    private final HashCache<Type, ExecutionBuilder> cache;

    public JvsEnrichMapper(String... tags) {
        EnrichExecutionBuilderMapper mapper = new EnrichExecutionBuilderMapper();
        if (ArrayUtil.nullOrEmpty(tags)) {
            String[] basic = {"basic"};
            mapper.setPredicate(mapper.getPredicate().and(new GroupTagPredicate(basic).or(new GroupTagNullPredicate())));
        } else {
            mapper.setPredicate(mapper.getPredicate().and(new GroupTagPredicate(tags).or(new GroupTagNullPredicate())));
        }
        String cacheKey = buildCacheKey(tags);
        cache = Type.getExecBuilderCache(cacheKey, mapper);
    }

    public JVS enrich(JVS doc) {
        Type type = doc.getType();
        if (type == null) return doc;

        ProjectionContext pc = new ProjectionContext();
        pc.source = doc;
        pc.target = new JVS();
        try {
            ExecutionBuilder builder = cache.get(type);
            if (builder != null && builder.getCurrentNode() != null) {
                builder.getCurrentNode().project(pc);
            }
        } catch (PropaccessError e) {
            log.error("Enrichment failed for type {}: {}", type.getName(), e.getMessage());
        }
        return pc.source;
    }

    private String buildCacheKey(String[] tags) {
        if (ArrayUtil.nullOrEmpty(tags)) return "JvsEnrichMapper:basic";
        StringBuilder sb = new StringBuilder("JvsEnrichMapper:");
        for (int i = 0; i < tags.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(tags[i]);
        }
        return sb.toString();
    }

    /**
     * Predicate that matches Groups whose tags overlap with the requested tag set.
     * Replicates hitorro-objretrieval's GroupTagPredicated exactly.
     */
    static class GroupTagPredicate implements Predicate<BaseT> {
        private final Set<String> tags;

        GroupTagPredicate(String[] tagsIn) {
            tags = MapUtil.getSet(tagsIn);
        }

        @Override
        public boolean test(BaseT b) {
            if (b instanceof Group group) {
                for (String s : group.getTags()) {
                    if (tags.contains(s)) return true;
                }
            }
            return false;
        }
    }

    /**
     * Predicate that matches Groups with null or empty tags.
     * Replicates hitorro-objretrieval's GroupTagNull exactly.
     */
    static class GroupTagNullPredicate implements Predicate<BaseT> {
        @Override
        public boolean test(BaseT b) {
            if (b instanceof Group group) {
                List<String> tags = group.getTags();
                return ListUtil.nullOrEmpty(tags);
            }
            return false;
        }
    }
}
