package com.earthinformatics.explorer.service;

import com.earthinformatics.explorer.client.OpenMeteoClient;
import com.earthinformatics.explorer.client.OpenMeteoClient.Reading;
import com.earthinformatics.explorer.config.CacheSupport;
import com.earthinformatics.explorer.config.Caches;
import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.Meta;
import com.earthinformatics.explorer.dto.SourceMeta;
import com.earthinformatics.explorer.dto.UpstreamResult;
import com.earthinformatics.explorer.dto.query.AtmosphericsQuery;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.AqiScale;
import com.earthinformatics.explorer.util.Geo;
import com.earthinformatics.explorer.util.Failures;
import com.earthinformatics.explorer.util.GeoJson;
import com.earthinformatics.explorer.util.SingleFlight;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Atmospheric domain: wind vectors, cloud cover, surface pressure and air quality.
 *
 * <p>Two Open-Meteo products, sampled over the same lattice and cached independently so a user
 * who only wants the AQI overlay never pays for the meteorological field.
 *
 * <p>Output shape: one Point feature per lattice cell. The renderer decides whether to draw a
 * point cloud, a heatmap or wind vectors; the service only guarantees the numbers, plus the
 * classification fields ({@code aqiCategory}, {@code aqiIndex}, {@code aqiColor}) so the legend
 * and the colours can never disagree.
 */
@Service
@Slf4j
public class AtmosphericsService {

    private final OpenMeteoClient openMeteoClient;
    private final CacheSupport cacheSupport;
    private final SingleFlight singleFlight;
    private final ExplorerProperties properties;

    public AtmosphericsService(OpenMeteoClient openMeteoClient, CacheSupport cacheSupport,
            SingleFlight singleFlight, ExplorerProperties properties) {
        this.openMeteoClient = openMeteoClient;
        this.cacheSupport = cacheSupport;
        this.singleFlight = singleFlight;
        this.properties = properties;
    }

    // ================================================== air quality (AQI)

    @Cacheable(cacheNames = Caches.AIR_QUALITY, key = "#query.cacheKey() + ':aqi'")
    public Mono<UpstreamResult> fetchAirQuality(AtmosphericsQuery query) {
        long startedAt = System.nanoTime();
        SourceMeta source = new SourceMeta("Open-Meteo Air Quality",
                "air-quality/domains=auto",
                properties.upstreams().openMeteo().airQualityUrl(), "CC-BY-4.0");

        return openMeteoClient.sampleGrid(OpenMeteoClient.Dataset.AIR_QUALITY, query.gridStep())
                .map(sample -> {
                    if (sample.unusable()) {
                        return UpstreamResult.failure(
                                gridMeta("Open-Meteo Air Quality", sample, query.gridStep(), 0),
                                source,
                                unusableReason(sample));
                    }
                    // A lattice with holes is still served, but never as a complete field.
                    return UpstreamResult.success(
                            flagIncomplete(buildAirQuality(sample.readings(), query,
                                    sample.chunks(), sample.failedChunks()), sample),
                            source, elapsed(startedAt));
                })
                .onErrorResume(error -> {
                    log.warn("Air-quality load failed: {}", error.toString());
                    return Mono.just(UpstreamResult.failure(
                            GeoJsonPayload.empty(Meta.live("Open-Meteo Air Quality")), source,
                            Failures.reason(error)));
                })
                .cache();
    }

    public Mono<GeoJsonPayload> airQuality(AtmosphericsQuery query) {
        String key = query.cacheKey() + ":aqi";
        return singleFlight.execute(key, () -> fetchAirQuality(query))
                .doOnNext(result -> cacheSupport.evictIfDegraded(Caches.AIR_QUALITY, key, result))
                .map(UpstreamResult::payload)
                .map(payload -> applyViewport(payload, query));
    }

    /**
     * Marks a payload degraded when the lattice it came from has holes in it.
     *
     * <p>Serving partial data is right; presenting it as a complete global field is not. A
     * degraded air-quality layer with two thirds of its chunks missing is still more useful than
     * an empty one, as long as the client is told.
     */
    static GeoJsonPayload flagIncomplete(GeoJsonPayload payload,
            OpenMeteoClient.GridSample sample) {
        if (!sample.partial()) {
            return payload;
        }
        return payload.withMeta(payload.meta().withDegraded(sample.incompleteness()));
    }

    /** Explains why a lattice came back empty, distinguishing rejection from no model data. */
    static String unusableReason(OpenMeteoClient.GridSample sample) {
        if (sample.failedChunks() > 0 && sample.droppedChunks() == 0) {
            return "all " + sample.chunks() + " lattice chunks were rejected by the provider "
                    + "(rate limit or upstream error)";
        }
        if (sample.droppedChunks() > 0 && sample.failedChunks() == 0) {
            return "the model has no data for any of the " + sample.chunks()
                    + " requested lattice chunks (cells outside model coverage)";
        }
        return sample.failedChunks() + " of " + sample.chunks()
                + " lattice chunks were rejected and " + sample.droppedChunks()
                + " resolved to no data";
    }

    /**
     * Metadata for a lattice that produced nothing at all. Built here rather than in the client
     * so the degraded payload still describes the request that failed.
     */
    private GeoJsonPayload gridMeta(String source, OpenMeteoClient.GridSample sample, double step,
            int features) {
        Meta meta = Meta.live(source)
                .with("gridStep", step)
                .with("chunksRequested", sample.chunks())
                .with("chunksFailed", sample.failedChunks())
                .with("chunksDropped", sample.droppedChunks());
        return GeoJsonPayload.empty(meta);
    }

    private GeoJsonPayload buildAirQuality(List<Reading> readings, AtmosphericsQuery query,
            int chunks, int failedChunks) {
        List<JsonNode> features = new ArrayList<>(readings.size());
        for (Reading reading : readings) {
            double usAqi = value(reading, "us_aqi");
            double pm25 = value(reading, "pm2_5");
            double pm10 = value(reading, "pm10");
            if (Double.isNaN(usAqi) && Double.isNaN(pm25) && Double.isNaN(pm10)) {
                continue;
            }
            // Fall back to the European composite when the US scale is unavailable, which happens
            // outside North America for some model runs.
            double composite = Double.isNaN(usAqi) ? value(reading, "european_aqi") : usAqi;
            AqiScale.Category category = AqiScale.classify(composite);

            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("pm2_5", round(pm25));
            detail.put("pm10", round(pm10));
            detail.put("usAqi", round(usAqi));
            detail.put("europeanAqi", round(value(reading, "european_aqi")));
            detail.put("ozone", round(value(reading, "ozone")));
            detail.put("nitrogenDioxide", round(value(reading, "nitrogen_dioxide")));
            detail.put("sulphurDioxide", round(value(reading, "sulphur_dioxide")));
            detail.put("carbonMonoxide", round(value(reading, "carbon_monoxide")));
            detail.put("dust", round(value(reading, "dust")));

            features.add(GeoJson.feature(
                    String.format(Locale.ROOT, "aqi-%d-%d", reading.row(), reading.column()),
                    reading.longitude(),
                    reading.latitude(),
                    GeoJson.props(
                            "type", "air-quality",
                            "aqi", round(composite),
                            "aqiIndex", category.index(),
                            "aqiCategory", category.label(),
                            "aqiColor", category.color(),
                            "advisory", category.advisory(),
                            "gridRow", reading.row(),
                            "gridColumn", reading.column(),
                            "observedAt", reading.observedAt() == null ? null
                                    : reading.observedAt().toString(),
                            "source", "Open-Meteo Air Quality",
                            "detail", detail)));
        }

        Meta meta = Meta.live("Open-Meteo Air Quality")
                .with("gridStep", query.gridStep())
                .with("chunksRequested", chunks)
                .with("chunksFailed", failedChunks)
                .with("legend", AqiScale.legend())
                .with("units", Map.of("pm2_5", "ug/m3", "pm10", "ug/m3", "aqi", "EPA index"))
                .with("fields", OpenMeteoClient.AIR_QUALITY_FIELDS);
        return GeoJsonPayload.of(features, meta);
    }

    // ============================================ surface meteorology + wind

    @Cacheable(cacheNames = Caches.FORECAST, key = "#query.cacheKey() + ':weather'")
    public Mono<UpstreamResult> fetchWeather(AtmosphericsQuery query) {
        long startedAt = System.nanoTime();
        SourceMeta source = new SourceMeta("Open-Meteo Forecast", "forecast",
                properties.upstreams().openMeteo().forecastUrl(), "CC-BY-4.0");

        return openMeteoClient.sampleGrid(OpenMeteoClient.Dataset.FORECAST, query.gridStep())
                .map(sample -> {
                    if (sample.unusable()) {
                        return UpstreamResult.failure(
                                gridMeta("Open-Meteo Forecast", sample, query.gridStep(), 0),
                                source,
                                unusableReason(sample));
                    }
                    return UpstreamResult.success(
                            flagIncomplete(buildWeather(sample.readings(), query, sample.chunks(),
                                    sample.failedChunks()), sample),
                            source, elapsed(startedAt));
                })
                .onErrorResume(error -> {
                    log.warn("Weather load failed: {}", error.toString());
                    return Mono.just(UpstreamResult.failure(
                            GeoJsonPayload.empty(Meta.live("Open-Meteo Forecast")), source,
                            Failures.reason(error)));
                })
                .cache();
    }

    public Mono<GeoJsonPayload> windAndWeather(AtmosphericsQuery query) {
        String key = query.cacheKey() + ":weather";
        return singleFlight.execute(key, () -> fetchWeather(query))
                .doOnNext(result -> cacheSupport.evictIfDegraded(Caches.FORECAST, key, result))
                .map(UpstreamResult::payload)
                .map(payload -> applyViewport(payload, query));
    }

    private GeoJsonPayload buildWeather(List<Reading> readings, AtmosphericsQuery query,
            int chunks, int failedChunks) {
        List<JsonNode> features = new ArrayList<>(readings.size());
        for (Reading reading : readings) {
            double speed = value(reading, "wind_speed_10m");
            double direction = value(reading, "wind_direction_10m");
            double cloud = value(reading, "cloud_cover");
            double pressure = value(reading, "surface_pressure");

            // Keep cells that carry at least one renderable quantity.
            if (Double.isNaN(speed) && Double.isNaN(cloud) && Double.isNaN(pressure)) {
                continue;
            }
            double speedClass = windClass(speed);
            features.add(GeoJson.feature(
                    String.format(Locale.ROOT, "met-%d-%d", reading.row(), reading.column()),
                    reading.longitude(),
                    reading.latitude(),
                    GeoJson.props(
                            "type", "weather-cell",
                            "windSpeed", round(speed),
                            "windSpeedUnit", "m/s",
                            "windDirection", round(direction),
                            "windCompass", Geo.compassLabel(direction),
                            "windGusts", round(value(reading, "wind_gusts_10m")),
                            "windClass", speedClass,
                            "cloudCover", round(cloud),
                            "surfacePressure", round(pressure),
                            "surfacePressureHpa", round(pressure / 100d),
                            "temperature", round(value(reading, "temperature_2m")),
                            "relativeHumidity", round(value(reading, "relative_humidity_2m")),
                            "gridRow", reading.row(),
                            "gridColumn", reading.column(),
                            "observedAt", reading.observedAt() == null ? null
                                    : reading.observedAt().toString(),
                            "source", "Open-Meteo Forecast")));
        }

        Meta meta = Meta.live("Open-Meteo Forecast")
                .with("gridStep", query.gridStep())
                .with("chunksRequested", chunks)
                .with("chunksFailed", failedChunks)
                .with("legend", "windClass: 0 calm|1 light|2 moderate|3 fresh|4 strong")
                .with("fields", OpenMeteoClient.FORECAST_FIELDS)
                .with("units", Map.of("windSpeed", "m/s", "surfacePressure", "Pa"));
        return GeoJsonPayload.of(features, meta);
    }

    // ===================================================== single coordinate

    /**
     * Everything the atmosphere looks like at one point - the endpoint behind the HUD's
     * "inspect this coordinate" readout.
     */
    public Mono<Map<String, Object>> inspect(double latitude, double longitude) {
        if (!Geo.isValidPosition(longitude, latitude)) {
            return Mono.error(new com.earthinformatics.explorer.error.InvalidRequestException(
                    "coordinates must satisfy -180<=lon<=180 and -90<=lat<=90"));
        }
        Mono<Map<String, Object>> air = openMeteoClient
                .samplePoint(OpenMeteoClient.Dataset.AIR_QUALITY, latitude, longitude)
                .map(reading -> {
                    Map<String, Object> values = new LinkedHashMap<>(reading.values());
                    AqiScale.Category category = AqiScale.classify(
                            reading.values().getOrDefault("us_aqi", Double.NaN));
                    values.put("aqiCategory", category.label());
                    values.put("aqiColor", category.color());
                    return Map.<String, Object>copyOf(values);
                })
                .defaultIfEmpty(Map.of());

        Mono<Map<String, Object>> weather = openMeteoClient
                .samplePoint(OpenMeteoClient.Dataset.FORECAST, latitude, longitude)
                .map(reading -> Map.<String, Object>copyOf(new LinkedHashMap<>(reading.values())))
                .defaultIfEmpty(Map.of());

        return Mono.zip(air, weather).map(tuple -> {
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("latitude", latitude);
            envelope.put("longitude", longitude);
            envelope.put("airQuality", tuple.getT1());
            envelope.put("weather", tuple.getT2());
            envelope.put("queriedAt", java.time.Instant.now().toString());
            return envelope;
        });
    }

    // ============================================================== summary

    @Cacheable(cacheNames = Caches.SUMMARIES, key = "'atmospherics'", cacheManager = "shortLivedCacheManager")
    public Mono<DomainSummary> summary() {
        AtmosphericsQuery query = new AtmosphericsQuery("all",
                properties.upstreams().openMeteo().gridStepDegrees(), null, null);
        return Mono.zip(airQuality(query), windAndWeather(query))
                .map(tuple -> {
                    GeoJsonPayload air = tuple.getT1();
                    GeoJsonPayload weather = tuple.getT2();
                    double worstAqi = Double.NaN;
                    for (JsonNode feature : air.features()) {
                        worstAqi = Math.max(worstAqi, GeoJson.propertyAsDouble(feature, "aqi", 0));
                    }
                    double maxWind = Double.NaN;
                    for (JsonNode feature : weather.features()) {
                        maxWind = Math.max(maxWind,
                                GeoJson.propertyAsDouble(feature, "windSpeed", 0));
                    }
                    AqiScale.Category category = AqiScale.classify(worstAqi);
                    return new DomainSummary("atmospherics", air.size(), air.meta().degraded(),
                            air.meta().fetchedAt(), worstAqi,
                            String.format(Locale.ROOT, "%s (%d)", category.label(),
                                    (int) Math.max(0, worstAqi)),
                            Meta.metrics("cells", air.size(),
                                    "worstAqiCategory", category.label(),
                                    "maxWindSpeedMs", round(maxWind),
                                    "pm25Max", round(worstPm(air, "pm2_5"))),
                            List.of());
                })
                .onErrorResume(error -> {
                    log.warn("atmospherics summary failed: {}", error.toString());
                    return Mono.just(DomainSummary.unavailable("atmospherics",
                            Failures.reason(error)));
                });
    }

    private static double worstPm(GeoJsonPayload air, String property) {
        double worst = Double.NaN;
        for (JsonNode feature : air.features()) {
            worst = Math.max(worst, GeoJson.propertyAsDouble(feature, property, 0));
        }
        return worst;
    }

    // ============================================================== helpers

    /** Beaufort-inspired banding used by the wind legend. */
    static int windClass(double metresPerSecond) {
        if (Double.isNaN(metresPerSecond) || metresPerSecond < 2) {
            return 0;
        }
        if (metresPerSecond < 6) {
            return 1;
        }
        if (metresPerSecond < 11) {
            return 2;
        }
        if (metresPerSecond < 17) {
            return 3;
        }
        return 4;
    }

    private static GeoJsonPayload applyViewport(GeoJsonPayload payload, AtmosphericsQuery query) {
        if (query.bbox() == null) {
            return payload;
        }
        List<JsonNode> visible = new ArrayList<>();
        for (JsonNode feature : payload.features()) {
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

    /** NaN-safe rounding; null is preserved so Jackson omits the field instead of writing NaN. */
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
