package com.earthinformatics.explorer.controller;

import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.TideStation;
import com.earthinformatics.explorer.dto.query.OceansQuery;
import com.earthinformatics.explorer.service.OceansService;
import com.earthinformatics.explorer.util.QueryParameters;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Oceans: tides, currents, sea-surface temperature and station water temperatures.
 *
 * <pre>
 * GET /api/v1/oceans/tides?days=2
 * GET /api/v1/oceans/currents?gridStep=10
 * GET /api/v1/oceans/sea-surface-temperature?gridStep=5&amp;bbox=...
 * GET /api/v1/oceans/water-temperature
 * GET /api/v1/oceans/stations
 * </pre>
 */
@RestController
@RequestMapping("/api/v1/oceans")
public class OceansController {

    private static final int MAX_TIDE_DAYS = 7;
    private static final double MIN_GRID_STEP = 1;
    private static final double MAX_GRID_STEP = 90;
    private static final double DEFAULT_GRID_STEP = 15;

    private final OceansService oceansService;

    public OceansController(OceansService oceansService) {
        this.oceansService = oceansService;
    }

    /**
     * NOAA CO-OPS tide predictions.
     *
     * @param days Look-ahead window; NOAA serves seven days, more is a 400.
     */
    @GetMapping("/tides")
    public Mono<GeoJsonPayload> tides(
            @RequestParam(defaultValue = "2") int days,
            @RequestParam(required = false) String bbox) {
        int window = QueryParameters.intInRange("days", days, 2, 1, MAX_TIDE_DAYS);
        return oceansService.tides(query("tides", DEFAULT_GRID_STEP, bbox), window);
    }

    /** Open-Meteo marine currents, sampled onto a lat/lon lattice. */
    @GetMapping("/currents")
    public Mono<GeoJsonPayload> currents(
            @RequestParam(defaultValue = "15") double gridStep,
            @RequestParam(required = false) String bbox) {
        return oceansService.currentsAndTemperature(
                query("currents", gridStep, bbox), Set.of("ocean-current"));
    }

    @GetMapping("/sea-surface-temperature")
    public Mono<GeoJsonPayload> seaSurfaceTemperature(
            @RequestParam(defaultValue = "5") double gridStep,
            @RequestParam(required = false) String bbox) {
        return oceansService.currentsAndTemperature(
                query("sst", gridStep, bbox), Set.of("sea-surface-temperature"));
    }

    /** Everything in the domain in one round trip: the default for the globe layer. */
    @GetMapping
    public Mono<GeoJsonPayload> all(
            @RequestParam(defaultValue = "15") double gridStep,
            @RequestParam(defaultValue = "2") int days,
            @RequestParam(required = false) String bbox) {
        OceansQuery query = query("all", gridStep, bbox);
        int window = QueryParameters.intInRange("days", days, 2, 1, MAX_TIDE_DAYS);
        return oceansService.tides(query, window)
                .zipWith(oceansService.currentsAndTemperature(query))
                .map(tuple -> Payloads.merge("oceans", tuple.getT1(), tuple.getT2()));
    }

    /** Latest observed water temperature at each configured station. */
    @GetMapping("/water-temperature")
    public Mono<GeoJsonPayload> waterTemperature() {
        return oceansService.waterTemperature();
    }

    /** Station inventory, so the UI can draw the tide network before any data arrives. */
    @GetMapping("/stations")
    public Mono<Map<String, Object>> stations() {
        List<TideStation> stations = oceansService.stations();
        return Mono.just(Map.of(
                "type", "FeatureCollection",
                "count", stations.size(),
                "stations", stations.stream()
                        .map(station -> Map.of(
                                "id", station.id(),
                                "name", station.name(),
                                "country", station.country(),
                                "latitude", station.latitude(),
                                "longitude", station.longitude()))
                        .toList()));
    }

    /** Single station detail, keyed by NOAA station id. */
    @GetMapping("/stations/{id}")
    public Mono<Map<String, Object>> station(@PathVariable String id) {
        return oceansService.stations().stream()
                .filter(candidate -> candidate.id().equalsIgnoreCase(id))
                .findFirst()
                .<Mono<Map<String, Object>>>map(station -> Mono.just(Map.of(
                        "id", station.id(),
                        "name", station.name(),
                        "country", station.country(),
                        "latitude", station.latitude(),
                        "longitude", station.longitude())))
                .orElseGet(() -> Mono.error(new com.earthinformatics.explorer.error
                        .InvalidRequestException("unknown station '" + id + "'")));
    }

    @GetMapping("/summary")
    public Mono<DomainSummary> summary() {
        return oceansService.summary();
    }

    // ------------------------------------------------------------------ helpers

    private OceansQuery query(String dataset, double gridStep, String bbox) {
        return new OceansQuery(dataset,
                QueryParameters.doubleInRange("gridStep", gridStep, DEFAULT_GRID_STEP,
                        MIN_GRID_STEP, MAX_GRID_STEP),
                QueryParameters.bbox(bbox));
    }
}
