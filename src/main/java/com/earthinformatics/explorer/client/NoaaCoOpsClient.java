package com.earthinformatics.explorer.client;

import com.earthinformatics.explorer.dto.TideStation;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.TideStations;
import com.earthinformatics.explorer.util.UpstreamRetry;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * NOAA CO-OPS (Tides & Currents) datagetter.
 *
 * <p>Upstream: {@code https://api.tidesandcurrents.noaa.gov/api/prod/datagetter}
 * <p>Documented at https://api.tidesandcurrents.noaa.gov/api/prod/
 *
 * <p>Published limits are 3 requests/second and 500/day per client for unregistered callers.
 * The global token bucket in {@code WebClientConfig} enforces the per-second part; the
 * cache TTL (15 min) keeps a 14-station refresh well under the daily quota.
 *
 * <p>Two products are used:
 * <ul>
 *   <li>{@code predictions} with {@code interval=hilo} - the high/low turning points that
 *       {@code TideMath} turns into a continuous curve.</li>
 *   <li>{@code water_temperature} - in-situ SST at the same stations, which anchors the
 *       satellite-derived sea-surface-temperature field to measurements.</li>
 * </ul>
 */
@Component
@Slf4j
public class NoaaCoOpsClient {

    private final WebClient webClient;
    private final ExplorerProperties properties;
    private final List<TideStation> stations;

    public NoaaCoOpsClient(WebClient webClient, ExplorerProperties properties) {
        this.webClient = webClient;
        this.properties = properties;
        this.stations = resolveStations(properties);
    }

    /** Configuration override wins; otherwise the curated global default set. */
    private static List<TideStation> resolveStations(ExplorerProperties properties) {
        List<ExplorerProperties.Upstreams.Noaa.Station> configured =
                properties.upstreams().noaa().stations();
        if (configured == null || configured.isEmpty()) {
            return TideStations.DEFAULT;
        }
        return configured.stream()
                .map(station -> new TideStation(station.id(), station.name(), station.country(),
                        station.latitude(), station.longitude()))
                .filter(TideStation::isValid)
                .toList();
    }

    public List<TideStation> stations() {
        return stations;
    }

    /**
     * High/low water predictions for one station covering the requested window.
     *
     * @param interval NOAA sampling interval; {@code hilo} for turning points only.
     * @param days     Number of days of predictions to request.
     */
    public Mono<JsonNode> predictions(String station, String interval, int days) {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        return request("predictions", params(
                "station", station,
                "interval", interval,
                "begin_date", today.toString(),
                "end_date", today.plusDays(Math.max(1, days)).toString(),
                "datum", "MSL",
                "time_zone", "gmt",
                "units", "metric",
                "format", "json"));
    }

    /** Hourly in-situ water temperature for a station (used as the SST validation layer). */
    public Mono<JsonNode> waterTemperature(String station) {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        return request("water_temperature", params(
                "station", station,
                "interval", "h",
                "begin_date", today.toString(),
                "end_date", today.toString(),
                "time_zone", "gmt",
                "units", "metric",
                "format", "json"));
    }

    /**
     * Fans out across every configured station, preserving order.
     *
     * <p>Concurrency is capped at {@code noaa.maxConcurrentRequests} (3 by default) so we stay
     * inside the documented limit even before the token bucket is consulted.
     */
    public Flux<StationResponse> predictionsForAllStations(int days) {
        AtomicInteger sequence = new AtomicInteger();
        return Flux.fromIterable(stations)
                .map(station -> new StationRequest(sequence.getAndIncrement(), station))
                .flatMap(request -> predictions(request.station().id(), "hilo", days)
                        .map(body -> new StationResponse(request, body, null))
                        .onErrorResume(error -> {
                            log.warn("NOAA predictions failed for {} ({}): {}",
                                    request.station().id(), request.station().name(),
                                    error.toString());
                            return Mono.just(new StationResponse(request, null, error.toString()));
                        }), Math.max(1, properties.upstreams().noaa().maxConcurrentRequests()));
    }

    private Mono<JsonNode> request(String product, Map<String, String> params) {
        if (!properties.upstreams().noaa().enabled()) {
            return Mono.empty();
        }
        StringBuilder uri = new StringBuilder(properties.upstreams().noaa().baseUrl())
                .append("?product=").append(product)
                .append("&application=").append(properties.upstreams().noaa().application());
        params.forEach((key, value) -> uri.append('&').append(key).append('=')
                .append(URLEncoder.encode(value, StandardCharsets.UTF_8)));

        // Logged at debug: CO-OPS answers with a bare 400 and a prose message for most query
        // mistakes, so being able to see the exact URI is the difference between a five-second
        // and a five-minute diagnosis.
        log.debug("NOAA {} {}", product, uri);

        return webClient.get()
                .uri(URI.create(uri.toString()))
                .retrieve()
                .bodyToMono(JsonNode.class)
                // CO-OPS reports a soft failure as HTTP 200 with {"error":{...}}, and it does not
                // use one consistent key for the result array: a date-ranged "predictions" request
                // answers "predictions", but a date-ranged "water_temperature" request answers
                // "data". Only the first was accepted, so every water-temperature station was
                // rejected as malformed and the endpoint reported zero readings as healthy.
                .flatMap(node -> node.has("error")
                        ? Mono.error(new UpstreamException(product, errorMessage(node)))
                        : hasPayload(node, product)
                                ? Mono.just(node)
                                : Mono.error(new UpstreamException(product,
                                        "response contained no result array (looked for '"
                                                + product + "' and 'data')")))
                .retryWhen(UpstreamRetry.backoff(
                        properties.resilience().retryBackoff(), properties.resilience().maxRetries()))
                .timeout(properties.resilience().responseTimeout());
    }

    /** Extracts CO-OPS' {@code error.message} field, falling back to the raw body. */
    private static String errorMessage(JsonNode node) {
        String message = node.path("error").path("message").asText("");
        return message.isBlank() ? node.toString() : message.strip();
    }

    /**
     * True when the body carries a result array.
     *
     * <p>CO-OPS is not consistent about the key: {@code predictions} comes back under
     * {@code "predictions"} for a date range and under {@code "data"} for a single date, and
     * {@code water_temperature} follows the same split. Both spellings are accepted, and an
     * {@code error} member is handled before this is consulted.
     */
    static boolean hasPayload(JsonNode node, String product) {
        return node.has(product) || node.has("data");
    }

    /**
     * A CO-OPS failure with a usable explanation.
     *
     * <p>Distinct from the transport exceptions so the logs say "station is not a valid station"
     * instead of an opaque {@code WebClientResponseException} dump, and so a decommissioned
     * station id is immediately diagnosable from {@code /api/v1/system/caches} or the log.
     */
    public static final class UpstreamException extends RuntimeException {

        private final String product;

        public UpstreamException(String product, String message) {
            super("NOAA " + product + " failed: " + message);
            this.product = product;
        }

        public String product() {
            return product;
        }
    }

    /** Index paired with the station so responses stay aligned after a partial failure. */
    public record StationRequest(int index, TideStation station) {
    }

    /**
     * @param response Raw NOAA body, or {@code null} when the station failed.
     * @param error    Failure description, or {@code null} on success.
     */
    public record StationResponse(StationRequest request, JsonNode response, String error) {

        public boolean ok() {
            return response != null;
        }

        public TideStation station() {
            return request.station();
        }
    }

    /** Tiny ordered map builder for the NOAA query parameters. */
    private static Map<String, String> params(String... keyValues) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            values.put(keyValues[i], keyValues[i + 1]);
        }
        return values;
    }
}
