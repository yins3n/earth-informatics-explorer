package com.earthinformatics.explorer.client;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.Geo;
import com.earthinformatics.explorer.util.GeoJson;
import com.earthinformatics.explorer.util.JsonSupport;
import com.earthinformatics.explorer.util.UpstreamRetry;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * NASA FIRMS - active fire detections (VIIRS/MODIS thermal anomalies).
 *
 * <p>Upstream: {@code https://firms.modaps.eosdis.nasa.gov/api/area/geojson/<MAP_KEY>/<bbox>/latest}
 * <p>Documented at https://firms.modaps.eosdis.nasa.gov/area_apidoc/
 *
 * <p>Three practical realities shape this client:
 * <ol>
 *   <li>A free MAP_KEY must be registered (the bundled {@code DEMO_KEY} is throttled to tiny
 *       bounding boxes), so the key is configuration, not a constant.</li>
 *   <li>DEMO_KEY and low-tier keys only answer {@code geojson} for small areas. When the
 *       response is not usable GeoJSON we transparently re-request {@code csv} and parse it -
 *       the area API's oldest and most widely supported format.</li>
 *   <li>{@code latest} returns the most recent complete satellite pass, which is exactly the
 *       "what is burning right now" semantic the globe needs; the day-indexed variant
 *       ({@code /days/day}) is used when a caller wants a wider historical window.</li>
 * </ol>
 */
@Component
@Slf4j
public class FirmsClient {

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HHmm");

    /** CSV column order published for the current VIIRS/MODIS products. */
    private static final List<String> FALLBACK_COLUMNS = List.of(
            "latitude", "longitude", "brightness", "date", "acq_time", "satellite",
            "confidence", "track", "frp", "version");

    private final WebClient webClient;
    private final ExplorerProperties properties;

    public FirmsClient(WebClient webClient, ExplorerProperties properties) {
        this.webClient = webClient;
        this.properties = properties;
    }

    /**
     * Retrieves active-fire detections for a bounding box, normalised to GeoJSON Features with
     * renderer-friendly property names.
     *
     * @param bbox Area of interest; FIRMS caps a single request at roughly 10 degrees square
     *             for DEMO_KEY and much larger for registered keys.
     * @param days Look-back window (1-10). When {@code 0} the most recent pass ({@code latest})
     *            is requested instead of a day window.
     */
    public Mono<JsonNode> fetchActiveFires(Geo.BBox bbox, int days) {
        ExplorerProperties.Upstreams.Firms config = properties.upstreams().firms();
        if (!config.enabled()) {
            return Mono.empty();
        }
        Geo.BBox box = bbox.snapped();
        String bboxSegment = "%s,%s,%s,%s".formatted(
                box.west(), box.south(), box.east(), box.north());
        String path = days > 0
                ? "%s/area/geojson/%s/%s/%d/%d".formatted(
                        config.baseUrl(), config.mapKey(), bboxSegment, Math.min(days, 10), days)
                : "%s/area/geojson/%s/%s/latest".formatted(config.baseUrl(), config.mapKey(),
                        bboxSegment);

        return webClient.get()
                .uri(path)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .filter(node -> node != null && node.isObject()
                        && node.has("features") && !node.path("features").isNull())
                .switchIfEmpty(Mono.defer(() -> csvFallback(box, days)))
                .map(node -> normalise(node))
                .retryWhen(UpstreamRetry.backoff(
                        properties.resilience().retryBackoff(), properties.resilience().maxRetries()))
                .timeout(properties.resilience().responseTimeout())
                .doOnNext(node -> log.debug("FIRMS returned {} detections", node.path("features").size()))
                .doOnError(error -> log.warn("FIRMS request failed: {}", error.toString()));
    }

    /**
     * Legacy {@code csv} endpoint + parser. Used whenever the geojson response is unusable,
     * which in practice means a restricted key rather than a provider outage.
     */
    private Mono<JsonNode> csvFallback(Geo.BBox box, int days) {
        ExplorerProperties.Upstreams.Firms config = properties.upstreams().firms();
        if (!config.csvFallbackEnabled()) {
            return Mono.empty();
        }
        String bboxSegment = "%s,%s,%s,%s".formatted(box.west(), box.south(), box.east(),
                box.north());
        String path = days > 0
                ? "%s/area/csv/%s/%s/%d/%d".formatted(config.baseUrl(), config.mapKey(),
                        bboxSegment, Math.min(days, 10), days)
                : "%s/area/csv/%s/%s/latest".formatted(config.baseUrl(), config.mapKey(),
                        bboxSegment);

        log.info("FIRMS geojson unusable, retrying area API in csv mode for {}", bboxSegment);
        return webClient.get()
                .uri(path)
                .retrieve()
                .bodyToMono(String.class)
                .map(this::parseCsv)
                .timeout(properties.resilience().responseTimeout());
    }

    /** Parses the FIRMS CSV dialect. Header-driven, so new columns are tolerated. */
    private JsonNode parseCsv(String csv) {
        if (csv == null || csv.isBlank()) {
            return JsonSupport.objectNode();
        }
        String[] lines = csv.strip().split("\\R");
        if (lines.length < 2) {
            return JsonSupport.objectNode();
        }
        List<String> header = new ArrayList<>(List.of(lines[0].split(",", -1)));
        List<String> columns = header.size() > 1 ? lowercase(header) : FALLBACK_COLUMNS;

        var features = JsonSupport.arrayNode();
        for (int i = 1; i < lines.length; i++) {
            if (lines[i].isBlank()) {
                continue;
            }
            String[] cells = lines[i].split(",", -1);
            Map<String, String> row = new HashMap<>();
            for (int column = 0; column < columns.size() && column < cells.length; column++) {
                row.put(columns.get(column), cells[column].strip());
            }
            JsonNode feature = featureFromRow(row);
            if (feature != null) {
                features.add(feature);
            }
        }
        var collection = JsonSupport.objectNode();
        collection.put("type", GeoJsonPayloadType.FEATURE_COLLECTION);
        collection.set("features", features);
        return collection;
    }

    private JsonNode featureFromRow(Map<String, String> row) {
        double latitude = parseDouble(row.get("latitude"));
        double longitude = parseDouble(row.get("longitude"));
        if (!Geo.isValidPosition(longitude, latitude)) {
            return null;
        }
        String acquiredAt = row.get("date");
        String acqTime = row.get("acq_time");
        long timestamp = parseAcquisition(acquiredAt, acqTime);
        String satellite = row.getOrDefault("satellite", "unknown");
        double frp = parseDouble(row.get("frp"));
        String confidence = row.getOrDefault("confidence", "nominal").toLowerCase(Locale.ROOT);

        String id = "fw-%s-%s%s-%d-%d".formatted(
                satellite, acquiredAt == null ? "na" : acquiredAt.replace("-", ""),
                acqTime == null ? "" : acqTime,
                Math.round(latitude * 100), Math.round(longitude * 100));

        return GeoJson.feature(id, longitude, latitude, GeoJson.props(
                "type", "wildfire",
                "satellite", satellite,
                "confidence", confidence,
                "confidenceRank", confidenceRank(confidence),
                "frp", frp,
                "brightness", parseDouble(row.get("brightness")),
                "track", parseDouble(row.get("track")),
                "acquiredAt", timestamp,
                "acquisitionTime", Instant.ofEpochMilli(timestamp).toString(),
                "version", row.getOrDefault("version", "unknown"),
                "source", "NASA FIRMS"));
    }

    /** Renames/annotates GeoJSON output while preserving every field the provider published. */
    private JsonNode normalise(JsonNode collection) {
        var features = JsonSupport.arrayNode();
        Set<String> seen = new LinkedHashSet<>();
        for (JsonNode feature : collection.path("features")) {
            double[] position = GeoJson.position(feature);
            if (position == null || !Geo.isValidPosition(position[0], position[1])) {
                continue;
            }
            JsonNode properties = feature.path("properties");
            String satellite = properties.path("satellite").asText("unknown");
            String confidence = properties.path("confidence").asText("nominal")
                    .toLowerCase(Locale.ROOT);
            long timestamp = parseAcquisition(
                    properties.path("acq_date").asText(null),
                    formatAcqTime(properties.path("acq_time").asText(null)));

            String id = "fw-%s-%d-%d-%d".formatted(satellite, timestamp,
                    Math.round(position[1] * 100), Math.round(position[0] * 100));
            if (!seen.add(id)) {
                // The same thermal anomaly appears in several satellite overpasses within a
                // window; de-duplicate by identity so the globe does not stack markers.
                continue;
            }

            features.add(GeoJson.feature(id, position[0], position[1], GeoJson.props(
                    "type", "wildfire",
                    "satellite", satellite,
                    "confidence", confidence,
                    "confidenceRank", confidenceRank(confidence),
                    "frp", properties.path("frp").asDouble(0),
                    "brightness", properties.path("brightness").asDouble(0),
                    "track", properties.path("track").asInt(0),
                    "acquiredAt", timestamp,
                    "acquisitionTime", Instant.ofEpochMilli(timestamp).toString(),
                    "version", properties.path("version").asText("unknown"),
                    "source", "NASA FIRMS")));
        }
        var out = JsonSupport.objectNode();
        out.put("type", GeoJsonPayloadType.FEATURE_COLLECTION);
        out.set("features", features);
        return out;
    }

    /** FIRMS allows {@code "h"/"n"/"l"} or the spelled-out form depending on product version. */
    private static int confidenceRank(String confidence) {
        return switch (confidence) {
            case "h", "high" -> 3;
            case "n", "nominal" -> 2;
            case "l", "low" -> 1;
            default -> 0;
        };
    }

    /** Parses {@code yyyy-MM-dd} + {@code HHmm} (UTC) into epoch milliseconds. */
    private static long parseAcquisition(String date, String time) {
        if (date == null || date.isBlank()) {
            return Instant.now().toEpochMilli();
        }
        try {
            LocalDate localDate = LocalDate.parse(date, DATE);
            LocalTime localTime = "24".equals(time) || time == null || time.isBlank()
                    ? LocalTime.of(23, 59)
                    : LocalTime.parse(time.length() == 3 ? "0" + time : time, TIME);
            return localDate.atTime(localTime).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (RuntimeException malformed) {
            return Instant.now().toEpochMilli();
        }
    }

    private static String formatAcqTime(String value) {
        if (value == null) {
            return null;
        }
        String digits = value.replaceAll("\\D", "");
        return digits.length() >= 4 ? digits.substring(0, 4) : value;
    }

    private static double parseDouble(String value) {
        if (value == null || value.isBlank()) {
            return Double.NaN;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException notANumber) {
            return Double.NaN;
        }
    }

    private static List<String> lowercase(List<String> values) {
        return values.stream().map(value -> value.strip().toLowerCase(Locale.ROOT)).toList();
    }

    /** URI used for the provenance block. */
    public String requestUrl(Geo.BBox box, int days) {
        ExplorerProperties.Upstreams.Firms config = properties.upstreams().firms();
        String bboxSegment = "%s,%s,%s,%s".formatted(box.west(), box.south(), box.east(),
                box.north());
        return days > 0
                ? "%s/area/geojson/%s/%s/%d/%d".formatted(config.baseUrl(),
                redact(config.mapKey()), bboxSegment, Math.min(days, 10), days)
                : "%s/area/geojson/%s/%s/latest".formatted(config.baseUrl(),
                redact(config.mapKey()), bboxSegment);
    }

    /** Never leak a MAP_KEY into logs or API responses. */
    private static String redact(String key) {
        if (key == null || key.isBlank()) {
            return "<unset>";
        }
        if (key.length() <= 4) {
            return "****";
        }
        return key.substring(0, 2) + "****" + key.substring(key.length() - 2);
    }

    /** Local constant so the client does not depend on the dto package for a string literal. */
    private static final class GeoJsonPayloadType {
        private static final String FEATURE_COLLECTION = "FeatureCollection";

        private GeoJsonPayloadType() {
        }
    }
}
