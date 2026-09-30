package com.earthinformatics.explorer.controller;

import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.query.AtmosphericsQuery;
import com.earthinformatics.explorer.service.AtmosphericsService;
import com.earthinformatics.explorer.util.QueryParameters;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Atmospherics: wind and surface weather, plus air quality.
 *
 * <pre>
 * GET /api/v1/atmospherics/wind?gridStep=15&amp;bbox=...
 * GET /api/v1/atmospherics/air-quality?gridStep=5
 * GET /api/v1/atmospherics/inspect?lat=51.5&amp;lon=-0.12
 * GET /api/v1/atmospherics/summary
 * </pre>
 */
@RestController
@RequestMapping("/api/v1/atmospherics")
public class AtmosphericsController {

    private static final double MIN_GRID_STEP = 1;
    private static final double MAX_GRID_STEP = 90;
    private static final double DEFAULT_GRID_STEP = 15;
    /** Air quality is denser than wind, so its default lattice is finer. */
    private static final double DEFAULT_AQI_GRID_STEP = 5;

    private final AtmosphericsService atmosphericsService;

    public AtmosphericsController(AtmosphericsService atmosphericsService) {
        this.atmosphericsService = atmosphericsService;
    }

    @GetMapping("/wind")
    public Mono<GeoJsonPayload> wind(
            @RequestParam(defaultValue = "15") double gridStep,
            @RequestParam(required = false) String bbox) {
        return atmosphericsService.windAndWeather(
                query("forecast", gridStep, "wind", bbox));
    }

    @GetMapping("/air-quality")
    public Mono<GeoJsonPayload> airQuality(
            @RequestParam(defaultValue = "5") double gridStep,
            @RequestParam(required = false) String bbox) {
        return atmosphericsService.airQuality(
                query("air-quality", gridStep, "aqi", bbox));
    }

    /** Wind, weather and air quality in one round trip: the default for the globe layer. */
    @GetMapping
    public Mono<GeoJsonPayload> all(
            @RequestParam(defaultValue = "15") double gridStep,
            @RequestParam(required = false) String bbox) {
        AtmosphericsQuery query = query("all", gridStep, null, bbox);
        return atmosphericsService.windAndWeather(query)
                .zipWith(atmosphericsService.airQuality(query))
                .map(tuple -> Payloads.merge("atmospherics", tuple.getT1(), tuple.getT2()));
    }

    /**
     * Everything the atmosphere looks like at one coordinate - the endpoint behind the HUD's
     * "inspect this point" readout.
     */
    @GetMapping("/inspect")
    public Mono<Map<String, Object>> inspect(
            @RequestParam(defaultValue = "0") double lat,
            @RequestParam(defaultValue = "0") double lon) {
        return atmosphericsService.inspect(lat, lon);
    }

    @GetMapping("/summary")
    public Mono<DomainSummary> summary() {
        return atmosphericsService.summary();
    }

    private AtmosphericsQuery query(String dataset, double gridStep, String metric,
            String bbox) {
        return new AtmosphericsQuery(dataset,
                QueryParameters.doubleInRange("gridStep", gridStep,
                        "air-quality".equals(dataset) ? DEFAULT_AQI_GRID_STEP : DEFAULT_GRID_STEP,
                        MIN_GRID_STEP, MAX_GRID_STEP),
                metric,
                QueryParameters.bbox(bbox));
    }
}
