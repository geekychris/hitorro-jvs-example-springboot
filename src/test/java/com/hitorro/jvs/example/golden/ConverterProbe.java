package com.hitorro.jvs.example.golden;

import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Feeds a fixed matrix of values through the JSON converter Spring MVC uses for responses and request
 * bodies, so the golden file pins the mapper's settings themselves (property order, dates, empty beans,
 * leniency, nesting limits), not only what today's endpoints happen to return. Uses only Spring and JDK
 * types, so it runs unchanged on the Jackson 2 and the Jackson 3 build.
 */
public final class ConverterProbe {

    private ConverterProbe() {}

    @SuppressWarnings("unchecked")
    public static HttpMessageConverter<Object> jsonConverter(RequestMappingHandlerAdapter adapter) {
        for (HttpMessageConverter<?> c : adapter.getMessageConverters()) {
            if (c.getClass().getName().contains("Jackson") && c.canWrite(LinkedHashMap.class, MediaType.APPLICATION_JSON)) {
                return (HttpMessageConverter<Object>) c;
            }
        }
        throw new IllegalStateException("no Jackson JSON converter in " + adapter.getMessageConverters());
    }

    public record Rec(String zeta, int alpha, Instant when, List<String> tags) {}

    public enum Color { RED, GREEN }

    public static class Bean {
        private final String zulu = "z";
        private final int alpha = 1;
        private final String nothing = null;
        private final Date created = new Date(1_700_000_000_123L);
        private final LocalDate day = LocalDate.of(2026, 11, 20);
        public String getZulu() { return zulu; }
        public int getAlpha() { return alpha; }
        public String getNothing() { return nothing; }
        public Date getCreated() { return created; }
        public LocalDate getDay() { return day; }
        public boolean isActive() { return true; }
    }

    public static class Empty {}

    /** Reading target: public fields, deterministic toString. */
    public static class Target {
        public int count;
        public String name;
        public List<String> tags;
        public Instant when;
        public Color color;
        public double ratio;
        @Override public String toString() {
            return "count=" + count + " name=" + name + " tags=" + tags + " when=" + when + " color=" + color + " ratio=" + ratio;
        }
    }

    public static void run(HttpMessageConverter<Object> c, GoldenRecorder g) {
        Map<String, Object> w = new LinkedHashMap<>();
        w.put("record", new Rec("z", 2, Instant.ofEpochMilli(1_700_000_000_123L), List.of("a", "b")));
        w.put("bean", new Bean());
        w.put("empty-bean", new Empty());
        w.put("instant", Instant.ofEpochSecond(1_700_000_000L, 123_456_789));
        w.put("local-date", LocalDate.of(2026, 11, 20));
        w.put("local-date-time", LocalDateTime.of(2026, 11, 20, 13, 45, 30, 500_000_000));
        w.put("offset-date-time", OffsetDateTime.of(2026, 11, 20, 13, 45, 30, 0, ZoneOffset.ofHours(-8)));
        w.put("duration", Duration.ofSeconds(90, 5));
        w.put("util-date", new Date(1_700_000_000_123L));
        w.put("optional-present", Optional.of("x"));
        w.put("optional-empty", Optional.empty());
        w.put("enum", Color.GREEN);
        w.put("uuid", UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));
        w.put("bytes", new byte[]{1, 2, 3, (byte) 255});
        w.put("chars", new char[]{'h', 'i'});
        w.put("big-decimal", new BigDecimal("1E+30"));
        w.put("big-decimal-plain", new BigDecimal("12.50"));
        w.put("big-integer", new BigInteger("123456789012345678901234567890"));
        w.put("double-nan", Double.NaN);
        w.put("double-inf", Double.POSITIVE_INFINITY);
        w.put("float", 1.1f);
        w.put("double", 0.1 + 0.2);
        w.put("long-max", Long.MAX_VALUE);
        Map<Object, Object> keys = new LinkedHashMap<>();
        keys.put(3, "int key");
        keys.put(Color.RED, "enum key");
        keys.put(null, "null key");
        w.put("map-odd-keys", keys);
        Map<String, Object> nulls = new LinkedHashMap<>();
        nulls.put("b", null);
        nulls.put("a", 1);
        w.put("map-null-values", nulls);
        Map<String, Object> sorted = new TreeMap<>(Map.of("b", 1, "a", 2));
        w.put("tree-map", sorted);
        w.put("string-escapes", "quote\" backslash\\ tab\t nl\n ctrl\u0001 unié emoji😀 slash/ lt< amp&");
        w.put("nested-400", nest(400));
        w.put("nested-1000", nest(1000));
        w.put("nested-1001", nest(1001));
        for (var e : w.entrySet()) g.put("converter-write " + e.getKey(), shorten(GoldenRecorder.write(c, e.getValue())));

        Map<String, String> r = new LinkedHashMap<>();
        r.put("target", "{\"count\":3,\"name\":\"n\",\"tags\":[\"x\"],\"when\":\"2026-11-20T13:45:30Z\",\"color\":\"RED\",\"ratio\":0.5}");
        r.put("target-unknown-property", "{\"count\":3,\"extra\":true}");
        r.put("target-null-primitive", "{\"count\":null,\"ratio\":null}");
        r.put("target-float-as-int", "{\"count\":1.9}");
        r.put("target-string-as-int", "{\"count\":\"12\"}");
        r.put("target-empty-string-as-int", "{\"count\":\"\"}");
        r.put("target-bool-as-int", "{\"count\":true}");
        r.put("target-int-as-string", "{\"name\":12}");
        r.put("target-single-as-array", "{\"tags\":\"one\"}");
        r.put("target-epoch-millis-instant", "{\"when\":1700000000123}");
        r.put("target-epoch-decimal-instant", "{\"when\":1700000000.123456789}");
        r.put("target-unknown-enum", "{\"color\":\"BLUE\"}");
        r.put("target-enum-index", "{\"color\":1}");
        r.put("target-trailing-tokens", "{\"count\":1} {\"count\":2}");
        r.put("target-comments", "{/*c*/\"count\":1}");
        r.put("target-single-quotes", "{'count':1}");
        r.put("target-duplicate-keys", "{\"count\":1,\"count\":2}");
        r.put("target-nan", "{\"ratio\":NaN}");
        r.put("target-empty", "");
        for (var e : r.entrySet()) g.put("converter-read " + e.getKey(), GoldenRecorder.read(c, Target.class, e.getValue()));

        Map<String, String> m = new LinkedHashMap<>();
        m.put("map", "{\"b\":1,\"a\":[1,2.5,\"s\",null,true],\"c\":{\"x\":1.10}}");
        m.put("big-numbers", "{\"i\":12345678901234567890,\"d\":1.0000000000000000001,\"e\":1e400}");
        m.put("leading-zero", "{\"n\":007}");
        m.put("trailing-comma", "{\"a\":1,}");
        m.put("trailing-garbage", "{\"a\":1}x");
        m.put("nested-500", nestJson(500));
        m.put("nested-501", nestJson(501));
        m.put("nested-1000", nestJson(1000));
        m.put("nested-1001", nestJson(1001));
        m.put("long-name", "{\"" + "n".repeat(50_001) + "\":1}");
        m.put("long-number", "{\"n\":" + "1".repeat(1001) + "}");
        for (var e : m.entrySet()) {
            String got = GoldenRecorder.read(c, Map.class, e.getValue());
            g.put("converter-read-map " + e.getKey(), shorten(got));
        }
    }

    private static String shorten(String s) {
        return s.length() > 300 ? s.substring(0, 300) + "...(" + s.length() + " chars)" : s;
    }

    /** {@code depth} nested arrays around a string. */
    private static Object nest(int depth) {
        Object v = "leaf";
        for (int i = 0; i < depth; i++) {
            List<Object> l = new ArrayList<>();
            l.add(v);
            v = l;
        }
        return v;
    }

    /** {@code depth} nested objects around a number. */
    private static String nestJson(int depth) {
        return "{\"a\":".repeat(depth) + "1" + "}".repeat(depth);
    }
}
