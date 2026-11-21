package com.hitorro.jvs.example.golden;

import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;

/**
 * Records REST responses as golden sections, or compares them with the recorded ones.
 *
 * <p>The same source compiles against the Jackson 2 build (main) and the Jackson 3 build: it uses only
 * Spring and JSONassert types. Record on main with {@code -Dgolden.record=true}; on the migrated build the
 * test compares every section with the file and fails listing each difference.
 *
 * <p>Bodies are compared byte for byte after {@link #normalize}: values that change from run to run
 * (timings, timestamps, absolute paths) are replaced, keeping the key order and formatting. A section
 * marked unordered (a response built from {@code Map.of}/{@code HashMap}, whose key order differs between
 * JVM runs on Jackson 2 as well) is compared with JSONassert STRICT: same keys, values, types and array
 * order, any object key order.
 */
public final class GoldenRecorder {

    public static final boolean RECORD = Boolean.getBoolean("golden.record");
    private static final String HEADER = "### ";

    private final Path file;
    private final Map<String, String> expected;
    private final Map<String, String> actual = new LinkedHashMap<>();
    private final Map<String, Boolean> unordered = new LinkedHashMap<>();
    private final List<UnaryOperator<String>> normalizers = new ArrayList<>();
    private final Map<String, UnaryOperator<String>> accepted = new LinkedHashMap<>();

    public GoldenRecorder(Path file) throws IOException {
        this.file = file;
        this.expected = RECORD ? Map.of() : parse(Files.readString(file, StandardCharsets.UTF_8));
    }

    /** The recorded text of a section (compare mode), or null. */
    public String expected(String name) {
        return expected.get(name);
    }

    /** Replace every number (or string) value of keys matching {@code keyRegex} with 0 (or "~"). */
    public GoldenRecorder volatileKeys(String keyRegex) {
        Pattern num = Pattern.compile("(\"(?:" + keyRegex + ")\"\\s*:\\s*)-?[0-9][0-9.eE+-]*");
        Pattern str = Pattern.compile("(\"(?:" + keyRegex + ")\"\\s*:\\s*)\"[^\"]*\"");
        normalizers.add(s -> str.matcher(num.matcher(s).replaceAll("$10")).replaceAll("$1\"~\""));
        return this;
    }

    public GoldenRecorder replace(String literal, String placeholder) {
        if (literal != null && !literal.isEmpty()) normalizers.add(s -> s.replace(literal, placeholder));
        return this;
    }

    public GoldenRecorder normalizer(UnaryOperator<String> op) {
        normalizers.add(op);
        return this;
    }

    /**
     * An accepted, documented difference: in section {@code name}, text matching {@code regex} is replaced
     * by {@code replacement} in both the golden and the actual output before comparing. Everything else in
     * the section must still be identical.
     */
    public GoldenRecorder acceptDifference(String name, String regex, String replacement) {
        Pattern p = Pattern.compile(regex);
        accepted.put(name, s -> p.matcher(s).replaceAll(replacement));
        return this;
    }

    public String normalize(String s) {
        for (UnaryOperator<String> n : normalizers) s = n.apply(s);
        return s;
    }

    // ---------------------------------------------------------------- REST

    /** Perform a request (following an async dispatch for streaming bodies) and record the response. */
    public String rest(MockMvc mvc, String name, MockHttpServletRequestBuilder request) throws Exception {
        return rest(mvc, name, request, false);
    }

    public String restUnordered(MockMvc mvc, String name, MockHttpServletRequestBuilder request) throws Exception {
        return rest(mvc, name, request, true);
    }

    private String rest(MockMvc mvc, String name, MockHttpServletRequestBuilder request, boolean anyKeyOrder)
            throws Exception {
        MvcResult result = mvc.perform(request).andReturn();
        if (result.getRequest().isAsyncStarted()) {
            result.getAsyncResult(120_000);
            result = mvc.perform(asyncDispatch(result)).andReturn();
        }
        MockHttpServletResponse r = result.getResponse();
        String body = r.getContentAsString(StandardCharsets.UTF_8);
        String ct = r.getContentType();
        String text = "status: " + r.getStatus() + "\ncontent-type: " + (ct == null ? "" : ct) + "\n" + body;
        put(name, text, anyKeyOrder);
        return body;
    }

    /** Record any text (a stored file, a converter result). */
    public void put(String name, String text, boolean anyKeyOrder) {
        if (actual.containsKey(name)) throw new IllegalStateException("duplicate golden section " + name);
        actual.put(name, normalize(text));
        unordered.put(name, anyKeyOrder);
    }

    public void put(String name, String text) {
        put(name, text, false);
    }

    // ---------------------------------------------------------------- converter probe

    /** What Boot's JSON converter writes for {@code value}, or the exception it throws. */
    public static String write(HttpMessageConverter<Object> c, Object value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpHeaders headers = new HttpHeaders();
        try {
            c.write(value, MediaType.APPLICATION_JSON, new HttpOutputMessage() {
                @Override public OutputStream getBody() { return out; }
                @Override public HttpHeaders getHeaders() { return headers; }
            });
            return "ok " + out.toString(StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "error " + e.getClass().getSimpleName();
        }
    }

    /** What Boot's JSON converter reads from {@code json} as {@code type}, written back as JSON. */
    public static String read(HttpMessageConverter<Object> c, Class<?> type, String json) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            @SuppressWarnings({"unchecked", "rawtypes"})
            Object v = ((HttpMessageConverter) c).read(type, new HttpInputMessage() {
                @Override public InputStream getBody() { return new ByteArrayInputStream(bytes); }
                @Override public HttpHeaders getHeaders() { return headers; }
            });
            return "ok " + describe(v);
        } catch (Exception e) {
            return "error " + e.getClass().getSimpleName();
        }
    }

    /** A value with the Java type of every map/list member, so Integer/Long/BigDecimal differences show. */
    public static String describe(Object v) {
        if (v == null) return "null";
        if (v instanceof Map<?, ?> m) {
            StringBuilder sb = new StringBuilder(v.getClass().getSimpleName()).append('{');
            m.forEach((k, x) -> sb.append(k).append('=').append(describe(x)).append(','));
            return sb.append('}').toString();
        }
        if (v instanceof List<?> l) {
            StringBuilder sb = new StringBuilder(v.getClass().getSimpleName()).append('[');
            l.forEach(x -> sb.append(describe(x)).append(','));
            return sb.append(']').toString();
        }
        return v.getClass().getSimpleName() + ":" + v;
    }

    // ---------------------------------------------------------------- finish

    /** Write the file (record mode) or return the differences (compare mode). */
    public List<String> finish() throws IOException {
        if (RECORD) {
            StringBuilder sb = new StringBuilder();
            actual.forEach((k, v) -> sb.append(HEADER).append(k).append('\n').append(v).append("\n\n"));
            Files.createDirectories(file.getParent());
            Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
            return List.of();
        }
        List<String> diffs = new ArrayList<>();
        for (String k : expected.keySet()) {
            if (!actual.containsKey(k)) diffs.add(k + ": not produced");
        }
        for (var e : actual.entrySet()) {
            String want = expected.get(e.getKey());
            if (want == null) {
                diffs.add(e.getKey() + ": not in golden file");
            } else if (!same(accept(e.getKey(), want), accept(e.getKey(), e.getValue()), unordered.get(e.getKey()))) {
                diffs.add(e.getKey() + ":\n  expected: " + abbreviate(want) + "\n  actual:   " + abbreviate(e.getValue()));
            }
        }
        return diffs;
    }

    private String accept(String name, String text) {
        UnaryOperator<String> op = accepted.get(name);
        return op == null ? text : op.apply(text);
    }

    private static boolean same(String want, String got, boolean anyKeyOrder) {
        if (want.equals(got)) return true;
        if (!anyKeyOrder) return false;
        // status and content-type lines must match exactly; the JSON body in any key order
        String[] w = want.split("\n", 3), g = got.split("\n", 3);
        if (w.length != 3 || g.length != 3 || !w[0].equals(g[0]) || !w[1].equals(g[1])) return false;
        try {
            JSONCompareResult r = JSONCompare.compareJSON(w[2], g[2], JSONCompareMode.STRICT);
            return r.passed();
        } catch (Exception e) {
            return false;
        }
    }

    private static String abbreviate(String s) {
        s = s.replace("\n", "\\n");
        return s.length() > 1500 ? s.substring(0, 1500) + "..." : s;
    }

    private static Map<String, String> parse(String content) {
        Map<String, String> m = new LinkedHashMap<>();
        String name = null;
        StringBuilder sb = new StringBuilder();
        for (String line : content.split("\n", -1)) {
            if (line.startsWith(HEADER)) {
                if (name != null) m.put(name, strip(sb, 2));
                name = line.substring(HEADER.length());
                sb.setLength(0);
            } else if (name != null) {
                sb.append(line).append('\n');
            }
        }
        if (name != null) m.put(name, strip(sb, 3));
        return m;
    }

    private static String strip(StringBuilder sb, int n) {
        // each section is written as text + "\n\n"; the last one also gets the newline the split re-adds
        String s = sb.toString();
        if (!s.endsWith("\n".repeat(n))) throw new IllegalStateException("malformed golden file near: " + s);
        return s.substring(0, s.length() - n);
    }
}
