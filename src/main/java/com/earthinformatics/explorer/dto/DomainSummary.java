package com.earthinformatics.explorer.dto;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Pre-aggregated per-domain roll-up consumed by the WebSocket broadcaster.
 *
 * <p>The tick loop runs every few seconds and must never be the place where expensive work
 * happens; therefore each domain publishes a {@code Summary} which is itself cached
 * (short TTL) and derived from the long-TTL dataset caches.
 *
 * @param domain      Logical layer group id, e.g. {@code tectonics}.
 * @param count       Feature count currently in the dataset.
 * @param degraded    Whether the backing dataset is degraded.
 * @param asOf        Timestamp of the underlying dataset.
 * @param maxValue    Domain headline metric (max magnitude, max FRP, peak AQI, ...).
 * @param latest      Small, renderable subset used for the "live pulse" overlay.
 */
public record DomainSummary(
        String domain,
        int count,
        boolean degraded,
        Instant asOf,
        double maxValue,
        String maxValueLabel,
        Map<String, Object> metrics,
        List<JsonNode> latest) {

    /**
     * Headline metric as JSON.
     *
     * <p>{@code NaN} is not a JSON number: Jackson would emit the bare token and break strict
     * parsers, so it is projected to {@code null} and the client reads {@code maxValueLabel}
     * instead.
     */
    @com.fasterxml.jackson.annotation.JsonProperty("maxValue")
    public Double maxValueOrNull() {
        return Double.isNaN(maxValue) || Double.isInfinite(maxValue) ? null : maxValue;
    }

    public static DomainSummary empty(String domain) {
        return new DomainSummary(domain, 0, false, Instant.now(), Double.NaN, "", Map.of(),
                List.of());
    }

    /**
     * A summary that could not be computed.
     *
     * <p>Distinct from {@link #empty(String)}: a roll-up that failed to build is not the same
     * claim as a roll-up that found nothing, and the dashboard shows a degraded badge for the
     * former. The reason is carried in {@code metrics} so an operator reading a tick dump can see
     * which upstream was at fault without turning on debug logging.
     */
    public static DomainSummary unavailable(String domain, String reason) {
        return new DomainSummary(domain, 0, true, Instant.now(), Double.NaN, "unavailable",
                Map.of("error", reason == null ? "unknown" : reason), List.of());
    }
}
