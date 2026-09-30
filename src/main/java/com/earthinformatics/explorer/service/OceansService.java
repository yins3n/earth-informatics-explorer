package com.earthinformatics.explorer.service;

import com.earthinformatics.explorer.client.NoaaCoOpsClient;
import com.earthinformatics.explorer.client.NoaaCoOpsClient.StationResponse;
import com.earthinformatics.explorer.client.OpenMeteoClient;
import com.earthinformatics.explorer.client.OpenMeteoClient.Reading;
import com.earthinformatics.explorer.config.CacheSupport;
import com.earthinformatics.explorer.config.Caches;
import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.Meta;
import com.earthinformatics.explorer.dto.SourceMeta;
import com.earthinformatics.explorer.dto.TideStation;
import com.earthinformatics.explorer.dto.UpstreamResult;
import com.earthinformatics.explorer.dto.query.OceansQuery;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.Geo;
import com.earthinformatics.explorer.util.Failures;
import com.earthinformatics.explorer.util.GeoJson;
import com.earthinformatics.explorer.util.SingleFlight;
import com.earthinformatics.explorer.util.TideMath;
import com.earthinformatics.explorer.util.TideStations;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Ocean domain: tidal heights, currents and sea-surface temperature.
 *
 * <p>Three distinct sources, deliberately separated so the expensive ones are independently
 * cacheable:
 * <ul>
 *   <li><b>Tides</b> - NOAA CO-OPS high/low turning points, converted into a continuous height by
 *       {@link TideMath}. One cached entry, 14 upstream requests, refreshed every 15 minutes.</li>
 *   <li><b>Currents + SST</b> - Open-Meteo marine API over a global lattice, one cached entry.</li>
 *   <li><b>In-situ SST</b> - NOAA water-temperature series, exposed as ground-truth validation
 *       points on top of the modelled field.</li>
 * </ul>
 *
 * <p>SST features are emitted as small <em>polygons</em> rather than points: the client renders
 * them as a single translucent primitive, which is the only way to get a smooth heatmap over the
 * ocean without shipping a raster tile pyramid.
 */
@Service
@Slf4j
public class OceansService {

    private final NoaaCoOpsClient noaaClient;
    private final OpenMeteoClient openMeteoClient;
    private final CacheSupport cacheSupport;
    private final SingleFlight singleFlight;
    private final ExplorerProperties properties;

    public OceansService(NoaaCoOpsClient noaaClient, OpenMeteoClient openMeteoClient,
            CacheSupport cacheSupport, SingleFlight singleFlight, ExplorerProperties properties) {
        this.noaaClient = noaaClient;
        this.openMeteoClient = openMeteoClient;
        this.cacheSupport = cacheSupport;
        this.singleFlight = singleFlight;
        this.properties = properties;
    }

    // ================================================================= tides

    @Cacheable(cacheNames = Caches.TIDES, key = "#days")
    public Mono<UpstreamResult> fetchTides(int days) {
        long startedAt = System.nanoTime();
        SourceMeta source = new SourceMeta("NOAA CO-OPS", "predictions/hilo",
                properties.upstreams().noaa().baseUrl(), "public-domain");

        return noaaClient.predictionsForAllStations(days)
                .collectList()
                .map(responses -> buildTides(responses))
                .map(payload -> UpstreamResult.success(payload, source, elapsed(startedAt)))
                .onErrorResume(error -> {
                    log.warn("Tide load failed: {}", error.toString());
                    return Mono.just(UpstreamResult.failure(
                            GeoJsonPayload.empty(Meta.live("NOAA CO-OPS")), source,
                            Failures.reason(error)));
                })
                .cache();
    }

    public Mono<GeoJsonPayload> tides(OceansQuery query, int days) {
        return singleFlight.execute("oceans:tides:" + days, () -> fetchTides(days))
                .doOnNext(result -> cacheSupport.evictIfDegraded(Caches.TIDES, days, result))
                .map(UpstreamResult::payload)
                .map(payload -> applyViewport(payload, query));
    }

    /**
     * Turns per-station hilo tables into features carrying an interpolated height, the tidal
     * phase, and the next turning point. Stations whose request failed are skipped and counted,
     * never fatal.
     */
    private GeoJsonPayload buildTides(List<StationResponse> responses) {
        List<JsonNode> features = new ArrayList<>(responses.size());
        Instant now = Instant.now();
        int failures = 0;

        for (StationResponse response : responses) {
            TideStation station = response.station();
            if (!response.ok()) {
                failures++;
                continue;
            }
            List<TideMath.TurningPoint> points = TideMath.parseHilo(response.response());
            if (points.size() < 2) {
                failures++;
                continue;
            }
            TideMath.Interpolation interpolation = TideMath.interpolate(points, now).orElse(null);

            Map<String, Object> predictions = new LinkedHashMap<>();
            predictions.put("turningPoints", points.stream().limit(6)
                    .map(point -> Map.of(
                            "time", point.time().toString(),
                            "type", point.type(),
                            "heightMeters", point.extreme()))
                    .toList());

            Map<String, Object> propertiesNode = GeoJson.props(
                    "type", "tide-station",
                    "stationId", station.id(),
                    "name", station.name(),
                    "country", station.country(),
                    "heightMeters", interpolation == null ? null : interpolation.heightMeters(),
                    "phase", interpolation == null ? "unknown" : interpolation.phase(),
                    "nextEventTime", interpolation == null ? null
                            : interpolation.nextTime().toString(),
                    "nextEventHeight", interpolation == null ? null : interpolation.nextHeight(),
                    "nextEventType", interpolation == null ? null : interpolation.nextType(),
                    "datum", "MSL",
                    "units", "metric",
                    "turningPointsAvailable", points.size(),
                    "predictions", predictions,
                    "source", "NOAA CO-OPS");

            features.add(GeoJson.feature("tide-" + station.id(),
                    station.longitude(), station.latitude(), propertiesNode));
        }

        Meta meta = Meta.live("NOAA CO-OPS")
                .with("stationFailures", failures)
                .with("stationsRequested", responses.size())
                .with("datum", "mean sea level")
                .with("interpolation", "linear between high/low turning points")
                .with("legend", "phase: rising|falling");
        if (features.isEmpty() && !responses.isEmpty()) {
            // Every station answered, none of them usably: a payload with zero features and
            // degraded=false is indistinguishable from "the sea is calm today", so say so.
            meta = meta.withDegraded("no station returned a usable prediction window");
        }
        return GeoJsonPayload.of(features, meta);
    }

    // ============================================ currents + sea surface temperature

    @Cacheable(cacheNames = Caches.MARINE, key = "#query.cacheKey()")
    public Mono<UpstreamResult> fetchMarine(OceansQuery query) {
        long startedAt = System.nanoTime();
        SourceMeta source = new SourceMeta("Open-Meteo Marine", "marine/current",
                properties.upstreams().openMeteo().marineUrl(), "CC-BY-4.0");

        return openMeteoClient.sampleGrid(OpenMeteoClient.Dataset.MARINE, query.gridStep())
                .map(sample -> {
                    if (sample.unusable()) {
                        return UpstreamResult.failure(
                                GeoJsonPayload.empty(Meta.live("Open-Meteo Marine")
                                        .with("gridStep", query.gridStep())
                                        .with("featureCount", 0)
                                        .with("chunksRequested", sample.chunks())
                                        .with("chunksFailed", sample.failedChunks())
                                        .with("chunksDropped", sample.droppedChunks())),
                                source,
                                AtmosphericsService.unusableReason(sample));
                    }
                    // A lattice with holes is still served, but never as a complete field.
                    return UpstreamResult.success(
                            AtmosphericsService.flagIncomplete(
                                    buildMarine(sample.readings(), query, sample.chunks(),
                                            sample.failedChunks()),
                                    sample),
                            source, elapsed(startedAt));
                })
                .onErrorResume(error -> {
                    log.warn("Marine load failed: {}", error.toString());
                    return Mono.just(UpstreamResult.failure(
                            GeoJsonPayload.empty(Meta.live("Open-Meteo Marine")), source,
                            Failures.reason(error)));
                })
                .cache();
    }

    /**
     * The marine upstream returns sea-surface temperature and currents in a single response, so
     * one fetch serves both. {@code types} narrows the payload to the feature types the caller
     * actually asked for: without it {@code /currents} and {@code /sea-surface-temperature}
     * returned byte-identical mixed payloads, and the client could not tell the two layers apart.
     * A null or empty set keeps every type, which is what the combined domain endpoint wants.
     */
    public Mono<GeoJsonPayload> currentsAndTemperature(OceansQuery query, Set<String> types) {
        return singleFlight.execute(query.cacheKey(), () -> fetchMarine(query))
                .doOnNext(result -> cacheSupport.evictIfDegraded(Caches.MARINE,
                        query.cacheKey(), result))
                .map(UpstreamResult::payload)
                .map(payload -> applyViewport(payload, query))
                .map(payload -> retainTypes(payload, types));
    }

    public Mono<GeoJsonPayload> currentsAndTemperature(OceansQuery query) {
        return currentsAndTemperature(query, Set.of());
    }

    private static GeoJsonPayload retainTypes(GeoJsonPayload payload, Set<String> types) {
        if (types == null || types.isEmpty()) {
            return payload;
        }
        List<JsonNode> kept = payload.features().stream()
                .filter(feature -> {
                    JsonNode type = feature.path("properties").path("type");
                    return type.isTextual() && types.contains(type.asText());
                })
                .toList();
        return payload.withFeatures(kept);
    }

    private GeoJsonPayload buildMarine(List<Reading> readings, OceansQuery query, int chunks,
            int failedChunks) {
        double halfCell = Math.max(0.5d, query.gridStep() / 2d);
        List<JsonNode> features = new ArrayList<>(readings.size());

        for (Reading reading : readings) {
            double sst = value(reading, "sea_surface_temperature");
            double currentVelocity = value(reading, "ocean_current_velocity");
            double currentDirection = value(reading, "ocean_current_direction");
            double waveHeight = value(reading, "wave_height");

            if (Double.isNaN(sst) && Double.isNaN(currentVelocity) && Double.isNaN(waveHeight)) {
                continue;
            }

            // Sea-surface temperature becomes a cell polygon: the client draws every polygon in
            // a single PerInstanceColorAppearance primitive, giving a continuous heatmap.
            if (!Double.isNaN(sst)) {
                features.add(GeoJson.feature(
                        String.format(Locale.ROOT, "sst-%d-%d", reading.row(), reading.column()),
                        GeoJson.cellPolygon(reading.longitude(), reading.latitude(), halfCell),
                        GeoJson.props(
                                "type", "sea-surface-temperature",
                                "value", round(sst),
                                "unit", "degC",
                                "gradient", "thermal",
                                "sstClass", sstClass(sst),
                                "gridRow", reading.row(),
                                "gridColumn", reading.column(),
                                "gridStep", query.gridStep(),
                                "observedAt", reading.observedAt() == null ? null
                                        : reading.observedAt().toString(),
                                "source", "Open-Meteo Marine")));
            }

            // Currents and wave state stay as points: they are drawn as directional vectors.
            if (!Double.isNaN(currentVelocity) || !Double.isNaN(waveHeight)) {
                features.add(GeoJson.feature(
                        String.format(Locale.ROOT, "cur-%d-%d", reading.row(), reading.column()),
                        reading.longitude(), reading.latitude(),
                        GeoJson.props(
                                "type", "ocean-current",
                                "velocity", round(currentVelocity),
                                "velocityUnit", "m/s",
                                "direction", round(currentDirection),
                                "compass", Geo.compassLabel(currentDirection),
                                "waveHeight", round(waveHeight),
                                "wavePeriod", round(value(reading, "wave_period")),
                                "waveDirection", round(value(reading, "wave_direction")),
                                "swellWaveHeight", round(value(reading, "swell_wave_height")),
                                "gridRow", reading.row(),
                                "gridColumn", reading.column(),
                                "source", "Open-Meteo Marine")));
            }
        }

        Meta meta = Meta.live("Open-Meteo Marine")
                .with("gridStep", query.gridStep())
                .with("geometryNote", "sst features are cell polygons; currents are points")
                .with("units", Map.of("sea_surface_temperature", "degC",
                        "ocean_current_velocity", "m/s", "wave_height", "m"))
                .with("legend", "sstClass: 0 freezing|1 cold|2 cool|3 temperate|4 warm|5 tropical");
        return GeoJsonPayload.of(features, meta);
    }

    // ============================================ in-situ water temperature

    @Cacheable(cacheNames = Caches.WATER_TEMPERATURE, key = "'stations'")
    public Mono<UpstreamResult> fetchWaterTemperature() {
        long startedAt = System.nanoTime();
        SourceMeta source = new SourceMeta("NOAA CO-OPS", "water_temperature",
                properties.upstreams().noaa().baseUrl(), "public-domain");

        return Flux.fromIterable(noaaClient.stations())
                // Concurrency 3 matches NOAA's published 3 requests/second ceiling.
                .flatMap(station -> noaaClient.waterTemperature(station.id())
                        .map(body -> new StationTemperature(station, body))
                        .onErrorResume(error -> {
                            log.debug("Water temperature unavailable for {}", station.id());
                            return Mono.empty();
                        }), 3)
                .collectList()
                .map(this::buildWaterTemperature)
                .map(payload -> UpstreamResult.success(payload, source, elapsed(startedAt)))
                .onErrorResume(error -> Mono.just(UpstreamResult.failure(
                        GeoJsonPayload.empty(Meta.live("NOAA CO-OPS")), source,
                        Failures.reason(error))))
                .cache();
    }

    public Mono<GeoJsonPayload> waterTemperature() {
        return singleFlight.execute("oceans:water-temperature", this::fetchWaterTemperature)
                .doOnNext(result -> cacheSupport.evictIfDegraded(Caches.WATER_TEMPERATURE,
                        "stations", result))
                .map(UpstreamResult::payload);
    }

    private GeoJsonPayload buildWaterTemperature(List<StationTemperature> readings) {
        List<JsonNode> features = new ArrayList<>();
        for (StationTemperature reading : readings) {
            TideStation station = reading.station();
            // CO-OPS uses two shapes for this product: the current one keys the array on "data"
            // and the rows on "v"/"t", while the legacy shape uses "water_temperature" with
            // "v_wtmp"/"v_date". Only the legacy keys were read, so every station produced nothing.
            JsonNode rows = reading.body().path("data").isArray()
                    ? reading.body().path("data")
                    : reading.body().path("water_temperature");
            for (JsonNode row : rows) {
                Double temperature = parseDouble(
                        firstText(row, "v", "v_wtmp"));
                if (temperature == null) {
                    continue;
                }
                String timestamp = firstText(row, "t", "v_date");
                features.add(GeoJson.feature("wt-%s-%s".formatted(station.id(), timestamp),
                        station.longitude(), station.latitude(),
                        GeoJson.props(
                                "type", "water-temperature",
                                "stationId", station.id(),
                                "name", station.name(),
                                "country", station.country(),
                                "value", temperature,
                                "unit", "degC",
                                "sstClass", sstClass(temperature),
                                "observedAt", timestamp,
                                "measured", true,
                                "source", "NOAA CO-OPS")));
                break; // one representative reading per station keeps the layer legible
            }
        }
        Meta meta = Meta.live("NOAA CO-OPS")
                .with("measured", true);
        if (features.isEmpty()) {
            // Reporting an empty layer as measured data is the failure this whole contract
            // exists to prevent: "we checked and there is no water temperature" is a different
            // statement from "no station serves this product".
            meta = meta.withDegraded("no station returned a water-temperature reading; NOAA "
                    + "offers in-situ water temperature at a minority of tide stations");
        }
        return GeoJsonPayload.of(features, meta);
    }

    /** First non-blank text among {@code keys}, or {@code null}. */
    private static String firstText(JsonNode row, String... keys) {
        for (String key : keys) {
            String value = row.path(key).asText(null);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    /** One station's water-temperature response. */
    public record StationTemperature(TideStation station, JsonNode body) {
    }

    // ============================================================== summary

    @Cacheable(cacheNames = Caches.SUMMARIES, key = "'oceans'", cacheManager = "shortLivedCacheManager")
    public Mono<DomainSummary> summary() {
        OceansQuery query = new OceansQuery("all",
                properties.upstreams().openMeteo().gridStepDegrees(), null);
        return Mono.zip(tides(query, 2), currentsAndTemperature(query))
                .map(tuple -> {
                    GeoJsonPayload tide = tuple.getT1();
                    GeoJsonPayload marine = tuple.getT2();
                    double extremeTide = Double.NaN;
                    for (JsonNode feature : tide.features()) {
                        extremeTide = Math.max(extremeTide,
                                GeoJson.propertyAsDouble(feature, "heightMeters", -99));
                    }
                    double hottest = Double.NaN;
                    for (JsonNode feature : marine.features()) {
                        if ("sea-surface-temperature".equals(
                                GeoJson.propertyAsString(feature, "type", ""))) {
                            hottest = Math.max(hottest, GeoJson.propertyAsDouble(feature, "value",
                                    -99));
                        }
                    }
                    return new DomainSummary("oceans", tide.size() + marine.size(),
                            tide.meta().degraded() || marine.meta().degraded(),
                            tide.meta().fetchedAt(), extremeTide,
                            String.format(Locale.ROOT, "%+.2f m", extremeTide),
                            Meta.metrics("tideStations", tide.size(),
                                    "marineFeatures", marine.size(),
                                    "maxSeaSurfaceTemperatureC", round(hottest)),
                            List.of());
                })
                .onErrorResume(error -> {
                    log.warn("oceans summary failed: {}", error.toString());
                    return Mono.just(DomainSummary.unavailable("oceans",
                            Failures.reason(error)));
                });
    }

    /** Station inventory, exposed so the UI can draw the station network before any tide data. */
    public List<TideStation> stations() {
        List<TideStation> configured = noaaClient.stations();
        return configured == null || configured.isEmpty() ? TideStations.DEFAULT : configured;
    }

    // ============================================================== helpers

    /**
     * Standard sea-surface-temperature banding (WMO-style) used by the legend.
     */
    static int sstClass(double celsius) {
        if (Double.isNaN(celsius) || celsius <= 0) {
            return 0;
        }
        if (celsius < 10) {
            return 1;
        }
        if (celsius < 18) {
            return 2;
        }
        if (celsius < 24) {
            return 3;
        }
        if (celsius < 29) {
            return 4;
        }
        return 5;
    }

    private static GeoJsonPayload applyViewport(GeoJsonPayload payload, OceansQuery query) {
        if (query.bbox() == null) {
            return payload;
        }
        List<JsonNode> visible = new ArrayList<>();
        for (JsonNode feature : payload.features()) {
            JsonNode geometry = feature.path("geometry");
            if ("Polygon".equals(geometry.path("type").asText())) {
                // Cell polygons straddle the viewport edge; keep them all (there are few).
                visible.add(feature);
                continue;
            }
            double[] position = GeoJson.position(feature);
            if (position != null && query.bbox().contains(position[0], position[1])) {
                visible.add(feature);
            }
        }
        // withFeatures last so featureCount describes the filtered collection, not the full
        // lattice: a panned view must not claim the whole globe's worth of cells.
        return payload
                .withMeta(payload.meta().with("viewport", query.bbox().toString()))
                .withFeatures(List.copyOf(visible));
    }

    private static double value(Reading reading, String key) {
        Double value = reading.values().get(key);
        return value != null ? value : Double.NaN;
    }

    private static Double parseDouble(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Double.parseDouble(raw.strip());
        } catch (NumberFormatException notANumber) {
            return null;
        }
    }

    private static Double round(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        }
        return Math.round(value * 100d) / 100d;
    }

    private static long elapsed(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1_000_000L;
    }
}
