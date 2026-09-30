package com.earthinformatics.explorer.controller;

import com.earthinformatics.explorer.config.CacheSupport;
import com.earthinformatics.explorer.config.WebSocketConfig;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.realtime.TelemetryBroadcaster;
import com.earthinformatics.explorer.util.SingleFlight;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Service introspection: version, layer catalogue, cache statistics and configuration echo.
 *
 * <p>The catalogue is what lets the frontend stay declarative. Rather than hard-coding layer
 * metadata in JavaScript, the UI fetches it once at boot and builds its toggles, legend swatches
 * and default views from the response. Adding a layer then touches one list instead of two
 * codebases.
 *
 * <p>Credentials are never echoed. {@link #configuration()} reports <em>which</em> providers have
 * credentials and which fall back to an anonymous or demo tier, without revealing the secrets.
 */
@RestController
@RequestMapping("/api/v1/system")
public class SystemController {

    private final ExplorerProperties properties;
    private final CacheSupport cacheSupport;
    private final TelemetryBroadcaster broadcaster;
    private final SingleFlight singleFlight;

    public SystemController(ExplorerProperties properties, CacheSupport cacheSupport,
            TelemetryBroadcaster broadcaster, SingleFlight singleFlight) {
        this.properties = properties;
        this.cacheSupport = cacheSupport;
        this.broadcaster = broadcaster;
        this.singleFlight = singleFlight;
    }

    @GetMapping("/info")
    public Mono<Map<String, Object>> info() {
        return Mono.just(Map.of(
                "name", "earth-informatics-explorer",
                "apiVersion", properties.api().version(),
                "status", "up",
                "serverTime", Instant.now(),
                "telemetry", Map.of(
                        "websocket", WebSocketConfig.TELEMETRY_PATH,
                        "sse", "/api/v1/telemetry/stream",
                        "tickIntervalMs", broadcaster.tickIntervalMillis())));
    }

    /**
     * Layer catalogue consumed by the UI.
     *
     * <p>Each entry carries the endpoint path, the query parameters it accepts, the feature
     * {@code type} to filter on, a colour ramp hint and a legend. Keeping this server-side means
     * the colour of "high magnitude quake" and the colour of "extreme AQI" are defined once.
     */
    @GetMapping("/layers")
    public Mono<Map<String, Object>> layers() {
        List<Map<String, Object>> layers = List.of(
                layer("earthquakes", "tectonics", "Earthquakes", "USGS",
                        "/api/v1/tectonics/earthquakes",
                        List.of("minMagnitude", "maxResults", "bbox", "feed"),
                        "earthquake", "#f97316", "Magnitude 2.5+", 5_000, 60_000),
                layer("volcanoes", "tectonics", "Active volcanoes", "NASA EONET",
                        "/api/v1/tectonics/volcanoes",
                        List.of("category", "days", "maxResults", "bbox"),
                        "volcano", "#ef4444", "Recent activity", 500, 15 * 60_000),
                layer("wind", "atmospherics", "Wind & weather", "Open-Meteo",
                        "/api/v1/atmospherics/wind", List.of("gridStep", "bbox"),
                        "weather-cell", "#38bdf8", "Speed m/s", 800, 10 * 60_000),
                layer("air-quality", "atmospherics", "Air quality", "Open-Meteo",
                        "/api/v1/atmospherics/air-quality", List.of("gridStep", "bbox"),
                        "air-quality", "#a855f7", "US AQI 0-500", 1_200, 10 * 60_000),
                layer("tides", "oceans", "Tide predictions", "NOAA CO-OPS",
                        "/api/v1/oceans/tides", List.of("days"),
                        "tide-station", "#22d3ee", "Height m", 60, 5 * 60_000),
                layer("currents", "oceans", "Surface currents", "Open-Meteo Marine",
                        "/api/v1/oceans/currents", List.of("gridStep", "bbox"),
                        "ocean-current", "#2dd4bf", "Speed m/s", 900, 10 * 60_000),
                layer("sea-surface-temperature", "oceans", "Sea surface temperature",
                        "Open-Meteo Marine", "/api/v1/oceans/sea-surface-temperature",
                        List.of("gridStep", "bbox"), "sea-surface-temperature",
                        "#fb923c", "Degrees C", 900, 10 * 60_000),
                layer("wildfires", "biosphere", "Active wildfires", "NASA FIRMS",
                        "/api/v1/biosphere/wildfires",
                        List.of("days", "minConfidence", "maxResults", "bbox"),
                        "wildfire", "#f43f5e", "FRP MW", 20_000, 5 * 60_000),
                layer("vegetation", "biosphere", "Vegetation (NDVI)", "NASA GIBS WMS",
                        "/api/v1/biosphere/vegetation", List.of("gridStep", "bbox"),
                        "vegetation", "#84cc16", "NDVI -1..1", 2_000, 15 * 60_000),
                layer("flights", "human-impact", "Live aircraft", "OpenSky Network",
                        "/api/v1/human-impact/flights",
                        List.of("bbox", "maxResults", "airborneOnly"),
                        "aircraft", "#60a5fa", "Altitude m", 8_000, 45_000),
                layer("vessels", "human-impact", "Live vessels", "AISStream.io",
                        "/api/v1/human-impact/vessels", List.of("bbox", "maxResults"),
                        "vessel", "#e2e8f0", "AIS status", 5_000, 30_000));

        return Mono.just(Map.of(
                "layers", layers,
                "domains", List.of("tectonics", "atmospherics", "oceans", "biosphere",
                        "human-impact"),
                "excluded", List.of("exosphere", "space-weather"),
                "telemetry", Map.of(
                        "websocket", WebSocketConfig.TELEMETRY_PATH,
                        "sse", "/api/v1/telemetry/stream")));
    }

    /**
     * Effective configuration with secrets redacted.
     *
     * <p>Reporting <em>tier</em> rather than <em>key</em> is deliberate: an operator debugging
     * "why is my wildfire layer empty" needs to know the deployment is still on the DEMO_KEY
     * tier, and does not need the key printed in a log aggregator.
     */
    @GetMapping("/configuration")
    public Mono<Map<String, Object>> configuration() {
        var firms = properties.upstreams().firms();
        var openSky = properties.upstreams().openSky();
        var ais = properties.upstreams().ais();
        return Mono.just(Map.of(
                "cache", Map.of(
                        "defaultTtl", properties.cache().defaultTtl(),
                        "shortTtl", properties.cache().shortTtl(),
                        "telemetryTtl", properties.cache().telemetryTtl(),
                        "defaultMaxSize", properties.cache().defaultMaxSize()),
                "providers", Map.of(
                        "firms", Map.of(
                                "enabled", firms.enabled(),
                                "credentialTier", firms.mapKey().isBlank() ? "none"
                                        : "DEMO_KEY".equals(firms.mapKey()) ? "demo" : "registered",
                                "defaultDays", firms.days()),
                        "openSky", Map.of(
                                "enabled", openSky.enabled(),
                                "credentialTier", openSky.clientId().isBlank() ? "anonymous"
                                        : "oauth2"),
                        "aisStream", Map.of(
                                "enabled", ais.enabled(),
                                "credentialTier", ais.apiKey().isBlank() ? "anonymous" : "keyed",
                                "maxTrackedVessels", ais.maxTrackedVessels()),
                        "openMeteo", Map.of(
                                "enabled", properties.upstreams().openMeteo().enabled(),
                                "credentialTier", "anonymous"),
                        "ndvi", Map.of(
                                "enabled", properties.upstreams().ndvi().enabled(),
                                "layerName", properties.upstreams().ndvi().layerName()),
                        "noaa", Map.of(
                                "enabled", properties.upstreams().noaa().enabled(),
                                "credentialTier", "anonymous",
                                "application", properties.upstreams().noaa().application())),
                "resilience", Map.of(
                        "tokensPerSecond", properties.resilience().tokensPerSecond(),
                        "burst", properties.resilience().burst(),
                        "maxRetries", properties.resilience().maxRetries(),
                        "responseTimeout", properties.resilience().responseTimeout())));
    }

    /** Cache and de-duplication statistics, for capacity planning. */
    @GetMapping("/caches")
    public Mono<Map<String, Object>> caches() {
        return Mono.just(Map.of(
                "statistics", cacheSupport.statistics(),
                "inFlightRequests", singleFlight.inFlightCount()));
    }

    // ------------------------------------------------------------------ helpers

    private static Map<String, Object> layer(String id, String domain, String label, String source,
            String endpoint, List<String> parameters, String featureType, String color,
            String legend, int typicalFeatures, long refreshMs) {
        // Map.ofEntries rather than Map.of: this is 11 pairs, past the 10-pair limit of the
        // varargs overload, and entry() keeps the key/value pairing readable.
        return Map.ofEntries(
                Map.entry("id", id),
                Map.entry("domain", domain),
                Map.entry("label", label),
                Map.entry("source", source),
                Map.entry("endpoint", endpoint),
                Map.entry("parameters", parameters),
                Map.entry("featureType", featureType),
                Map.entry("color", color),
                Map.entry("legend", legend),
                Map.entry("typicalFeatures", typicalFeatures),
                Map.entry("refreshMs", refreshMs));
    }
}
