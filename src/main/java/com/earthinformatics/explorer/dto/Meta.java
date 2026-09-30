package com.earthinformatics.explorer.dto;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable {@code meta} member attached to every GeoJSON response.
 *
 * <p>RFC 7946 permits foreign members alongside {@code type}/{@code features}, which lets us
 * keep strict GeoJSON compatibility (Cesium's {@code GeoJsonDataSource} parses it happily)
 * while still shipping provenance, degradation state and query echo to the client.
 */
public record Meta(
        List<String> sources,
        Instant fetchedAt,
        boolean degraded,
        String error,
        long upstreamMillis,
        Map<String, Object> details) {

    /** Starts a meta block stamped with the current instant. */
    public static Meta live(String... sources) {
        return new Meta(List.of(sources), Instant.now(), false, null, 0L, Map.of());
    }

    /** Starts a meta block that will be marked degraded when {@link #withDegraded} is called. */
    public static Meta forSources(String... sources) {
        return live(sources);
    }

    public Meta with(String key, Object value) {
        Map<String, Object> copy = new LinkedHashMap<>(details);
        copy.put(key, value);
        return new Meta(sources, fetchedAt, degraded, error, upstreamMillis, unmodifiable(copy));
    }

    /**
     * Null-tolerant {@code Map.of}.
     *
     * <p>{@code Map.of} throws a NullPointerException on a null value, which turns "this metric
     * was not measurable" into a 500. Diagnostics are allowed to be absent, so details and
     * summary metrics are assembled here instead.
     */
    public static Map<String, Object> metrics(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("metrics() needs key/value pairs");
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return unmodifiable(map);
    }

    private static Map<String, Object> unmodifiable(Map<String, Object> source) {
        // Collections.unmodifiableMap, not Map.copyOf: the latter rejects null values, and a
        // detail that is explicitly null is meaningful ("no data"), not a programming error.
        return Collections.unmodifiableMap(source);
    }

    public Meta withSources(List<String> additional) {
        return new Meta(
                List.copyOf(additional), fetchedAt, degraded, error, upstreamMillis, details);
    }

    public Meta withUpstreamMillis(long millis) {
        return new Meta(sources, fetchedAt, degraded, error, millis, details);
    }

    public Meta withDegraded(String reason) {
        String message = reason == null ? "upstream unavailable" : reason;
        return new Meta(sources, Instant.now(), true, message, upstreamMillis,
                details.isEmpty() ? Map.of("degraded", Boolean.TRUE) : details);
    }

    /**
     * Combines the degradation state of two merged sources. Used when a controller concatenates
     * several payloads into one layer: a layer is only as trustworthy as its least trustworthy
     * contributor, so a single degraded sub-source degrades the merged result.
     */
    public Meta withDegradedIfEither(boolean degraded) {
        return degraded ? withDegraded("one or more sources were unavailable") : this;
    }

    /** Numeric accessor tolerant of missing / non-numeric detail values. */
    public double detailAsDouble(String key, double fallback) {
        Object value = details.get(key);
        return value instanceof Number number ? number.doubleValue() : fallback;
    }
}
