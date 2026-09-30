package com.earthinformatics.explorer.controller;

import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.query.BiosphereQuery;
import com.earthinformatics.explorer.service.BiosphereService;
import com.earthinformatics.explorer.util.QueryParameters;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Biosphere: active wildfires and vegetation vigour.
 *
 * <pre>
 * GET /api/v1/biosphere/wildfires?days=1&amp;minConfidence=2
 * GET /api/v1/biosphere/vegetation?gridStep=15&amp;bbox=...
 * GET /api/v1/biosphere/summary
 * </pre>
 */
@RestController
@RequestMapping("/api/v1/biosphere")
public class BiosphereController {

    private static final int MAX_DAYS = 10;
    private static final int MIN_CONFIDENCE = 0;
    private static final int MAX_CONFIDENCE = 3;
    private static final double MIN_GRID_STEP = 1;
    private static final double MAX_GRID_STEP = 90;

    private final BiosphereService biosphereService;

    public BiosphereController(BiosphereService biosphereService) {
        this.biosphereService = biosphereService;
    }

    /**
     * @param days          FIRMS look-back window in days (0 = most recent pass only).
     * @param minConfidence FIRMS confidence rank floor: 0 any, 1 low, 2 nominal, 3 high.
     * @param bbox          Post-cache viewport filter.
     */
    @GetMapping("/wildfires")
    public Mono<GeoJsonPayload> wildfires(
            @RequestParam(defaultValue = "1") int days,
            @RequestParam(defaultValue = "0") int minConfidence,
            @RequestParam(defaultValue = "20000") int maxResults,
            @RequestParam(required = false) String bbox) {
        BiosphereQuery query = new BiosphereQuery("wildfires",
                QueryParameters.intInRange("days", days, 1, 0, MAX_DAYS),
                QueryParameters.intInRange("minConfidence", minConfidence, 0, MIN_CONFIDENCE,
                        MAX_CONFIDENCE),
                15,
                QueryParameters.bbox(bbox));
        return biosphereService.wildfires(query)
                .map(payload -> cap(payload, maxResults, "maxResults", 1, 100_000));
    }

    /** NDVI sampled from the configured imagery server. */
    @GetMapping("/vegetation")
    public Mono<GeoJsonPayload> vegetation(
            @RequestParam(defaultValue = "15") double gridStep,
            @RequestParam(required = false) String bbox) {
        return biosphereService.vegetation(new BiosphereQuery("vegetation", 0, 0,
                QueryParameters.doubleInRange("gridStep", gridStep, 15, MIN_GRID_STEP,
                        MAX_GRID_STEP),
                QueryParameters.bbox(bbox)));
    }

    /** Wildfires and vegetation in one round trip. */
    @GetMapping
    public Mono<GeoJsonPayload> all(
            @RequestParam(defaultValue = "1") int days,
            @RequestParam(defaultValue = "0") int minConfidence,
            @RequestParam(defaultValue = "15") double gridStep,
            @RequestParam(required = false) String bbox) {
        BiosphereQuery query = new BiosphereQuery("all",
                QueryParameters.intInRange("days", days, 1, 0, MAX_DAYS),
                QueryParameters.intInRange("minConfidence", minConfidence, 0, MIN_CONFIDENCE,
                        MAX_CONFIDENCE),
                QueryParameters.doubleInRange("gridStep", gridStep, 15, MIN_GRID_STEP,
                        MAX_GRID_STEP),
                QueryParameters.bbox(bbox));
        return biosphereService.wildfires(query)
                .zipWith(biosphereService.vegetation(query))
                .map(tuple -> Payloads.merge("biosphere", tuple.getT1(), tuple.getT2()));
    }

    /**
     * NDVI layer names the configured WMS actually publishes.
     *
     * <p>Layer names are deployment-specific and change when NASA promotes a product, so the UI
     * offers whatever the server reports rather than a hard-coded list that silently returns
     * blank tiles.
     */
    @GetMapping("/vegetation/layers")
    public Mono<List<String>> vegetationLayers() {
        return biosphereService.vegetationLayers();
    }

    @GetMapping("/summary")
    public Mono<DomainSummary> summary() {
        return biosphereService.summary();
    }

    @GetMapping("/providers")
    public Mono<Map<String, Object>> providers() {
        return Mono.just(Map.of(
                "wildfires", Map.of(
                        "name", "NASA FIRMS",
                        "url", "https://firms.modaps.eosdis.nasa.gov/",
                        "note", "MAP_KEY defaults to DEMO_KEY: register a free key for global "
                                + "coverage and higher rate limits"),
                "vegetation", Map.of(
                        "name", "NASA GIBS / compatible WMS",
                        "url", "https://gibs.earthdata.nasa.gov/wms/epsg4326/best",
                        "note", "layer names are discovered via GetCapabilities")));
    }

    private static GeoJsonPayload cap(GeoJsonPayload payload, int maxResults, String name,
            int min, int max) {
        int cap = QueryParameters.intInRange(name, maxResults, 20_000, min, max);
        if (payload.size() <= cap) {
            return payload;
        }
        // withFeatures last: it is what stamps featureCount from the list actually returned.
        // A withMeta after it would restore the pre-cap count and the page would under-report.
        return payload
                .withMeta(payload.meta().with("truncated", true)
                        .with("maxResults", cap)
                        .with("available", payload.size()))
                .withFeatures(payload.features().subList(0, cap));
    }
}
