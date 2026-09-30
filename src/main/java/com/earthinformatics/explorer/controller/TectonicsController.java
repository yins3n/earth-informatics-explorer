package com.earthinformatics.explorer.controller;

import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.query.TectonicsQuery;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.service.TectonicsService;
import com.earthinformatics.explorer.util.Geo;
import com.earthinformatics.explorer.util.QueryParameters;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Tectonics: live earthquakes and active volcanic events.
 *
 * <pre>
 * GET /api/v1/tectonics/earthquakes?minMagnitude=2.5&amp;bbox=-180,-90,180,90
 * GET /api/v1/tectonics/volcanoes?days=30
 * GET /api/v1/tectonics/summary
 * </pre>
 *
 * <p>Every endpoint returns a GeoJSON {@code FeatureCollection} with the {@code meta} foreign
 * member attached; nothing here throws on upstream failure because the services degrade to a
 * valid, empty, explicitly-flagged collection instead.
 */
@RestController
@RequestMapping("/api/v1/tectonics")
public class TectonicsController {

    /** Lower bound on the magnitude slider, matching the record's own clamp. */
    private static final double MIN_MAGNITUDE = -1;
    private static final double MAX_MAGNITUDE = 10;
    private static final int MIN_RESULTS = 1;
    private static final int MAX_RESULTS = 20_000;
    private static final int MAX_DAYS = 365;

    private final TectonicsService tectonicsService;
    private final ExplorerProperties properties;

    public TectonicsController(TectonicsService tectonicsService, ExplorerProperties properties) {
        this.tectonicsService = tectonicsService;
        this.properties = properties;
    }

    /**
     * @param feed       USGS feed name, or {@code auto} for the configured default.
     * @param minMagnitude Post-cache magnitude filter; the upstream document is fetched whole.
     * @param maxResults Post-cache cap on returned features.
     * @param bbox       {@code west,south,east,north}; post-cache viewport filter.
     */
    @GetMapping("/earthquakes")
    public Mono<GeoJsonPayload> earthquakes(
            @RequestParam(required = false) String feed,
            @RequestParam(required = false) Double minMagnitude,
            @RequestParam(defaultValue = "2000") int maxResults,
            @RequestParam(required = false) String bbox) {
        TectonicsQuery query = new TectonicsQuery(feed, "volcanoes", 30,
                QueryParameters.doubleInRange("minMagnitude", minMagnitude, 0, MIN_MAGNITUDE,
                        MAX_MAGNITUDE),
                QueryParameters.intInRange("maxResults", maxResults, 2_000, MIN_RESULTS,
                        MAX_RESULTS),
                QueryParameters.bbox(bbox));
        return tectonicsService.earthquakes(query);
    }

    /**
     * Default window is a year, not 30 days: EONET's volcano feed is event-driven, so a typical
     * month contains zero open events and the layer looks broken. A year reliably returns
     * currently-reported volcanoes.
     */
    @GetMapping("/volcanoes")
    public Mono<GeoJsonPayload> volcanoes(
            @RequestParam(required = false) String category,
            @RequestParam(defaultValue = "365") int days,
            @RequestParam(defaultValue = "1000") int maxResults,
            @RequestParam(required = false) String bbox) {
        TectonicsQuery query = new TectonicsQuery("auto", category,
                QueryParameters.intInRange("days", days, 30, 1, MAX_DAYS),
                0,
                QueryParameters.intInRange("maxResults", maxResults, 1_000, MIN_RESULTS,
                        MAX_RESULTS),
                QueryParameters.bbox(bbox));
        return tectonicsService.volcanoes(query);
    }

    /** Combined layer: earthquakes and volcanoes in a single round trip. */
    @GetMapping
    public Mono<GeoJsonPayload> all(
            @RequestParam(required = false) Double minMagnitude,
            @RequestParam(defaultValue = "30") int days,
            @RequestParam(defaultValue = "2000") int maxResults,
            @RequestParam(required = false) String bbox) {
        TectonicsQuery query = new TectonicsQuery("auto", "volcanoes",
                QueryParameters.intInRange("days", days, 30, 1, MAX_DAYS),
                QueryParameters.doubleInRange("minMagnitude", minMagnitude, 0, MIN_MAGNITUDE,
                        MAX_MAGNITUDE),
                QueryParameters.intInRange("maxResults", maxResults, 2_000, MIN_RESULTS,
                        MAX_RESULTS),
                QueryParameters.bbox(bbox));
        // Earthquakes and volcanoes are queried with their own windows: a 30-day earthquake
        // query and a 30-day volcano query share a number but not a meaning, and reusing the
        // quake window here returned an empty volcano set on nearly every call.
        TectonicsQuery volcanoQuery = new TectonicsQuery("auto", "volcanoes",
                properties.upstreams().eonet().defaultDays(),
                0,
                query.maxResults(),
                query.bbox());
        return Mono.zip(
                        tectonicsService.earthquakes(query),
                        tectonicsService.volcanoes(volcanoQuery))
                .map(tuple -> Payloads.merge("tectonics", tuple.getT1(), tuple.getT2()));
    }

    @GetMapping("/summary")
    public Mono<DomainSummary> summary() {
        return tectonicsService.summary();
    }

    /** Feeds the UI can offer in its source picker, resolved from configuration. */
    @GetMapping("/feeds")
    public Mono<java.util.Map<String, Object>> feeds() {
        return Mono.just(java.util.Map.of(
                "earthquakes", java.util.List.of("auto", "all_hour", "all_day", "all_week",
                        "all_month", "2.5_day", "1.0_day", "significant_month"),
                "volcanoes", java.util.List.of("volcanoes", "severeStorms", "seaLakeIce",
                        "wildfires", "earthquakes")));
    }

    /** Bounding box helper used by the globe to zoom to the full extent of a layer. */
    @GetMapping("/viewport")
    public Mono<java.util.Map<String, Object>> viewport() {
        Geo.BBox global = Geo.BBox.global();
        return Mono.just(java.util.Map.of(
                "global", java.util.List.of(global.west(), global.south(), global.east(),
                        global.north()),
                "crossesAntimeridian", global.isWrapped()));
    }
}
