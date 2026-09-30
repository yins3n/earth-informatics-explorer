package com.earthinformatics.explorer.service;

import com.earthinformatics.explorer.client.FirmsClient;
import com.earthinformatics.explorer.client.NdviWmsClient;
import com.earthinformatics.explorer.client.NdviWmsClient.NdviSample;
import com.earthinformatics.explorer.config.CacheSupport;
import com.earthinformatics.explorer.config.Caches;
import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.Meta;
import com.earthinformatics.explorer.dto.SourceMeta;
import com.earthinformatics.explorer.dto.UpstreamResult;
import com.earthinformatics.explorer.dto.query.BiosphereQuery;
import com.earthinformatics.explorer.properties.ExplorerProperties;
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
 * Biosphere domain: active wildfires (NASA FIRMS) and vegetation health (NDVI).
 *
 * <p>Scope note - this deployment deliberately excludes exospheric and space-weather products;
 * the domain is strictly surface biosphere: what is burning and how green the planet is.
 *
 * <p>FIRMS detections are already normalised by {@link FirmsClient} (confidence ranked, FRP
 * parsed, acquisition timestamps converted); this service adds the viewport/confidence filter
 * and the summary roll-up. NDVI is sampled from a WMS and classified with the standard
 * vegetation-index thresholds.
 */
@Service
@Slf4j
public class BiosphereService {

    private final FirmsClient firmsClient;
    private final NdviWmsClient ndviWmsClient;
    private final CacheSupport cacheSupport;
    private final SingleFlight singleFlight;
    private final ExplorerProperties properties;

    public BiosphereService(FirmsClient firmsClient, NdviWmsClient ndviWmsClient,
            CacheSupport cacheSupport, SingleFlight singleFlight, ExplorerProperties properties) {
        this.firmsClient = firmsClient;
        this.ndviWmsClient = ndviWmsClient;
        this.cacheSupport = cacheSupport;
        this.singleFlight = singleFlight;
        this.properties = properties;
    }

    // ============================================================ wildfires

    @Cacheable(cacheNames = Caches.WILDFIRES, key = "#query.wildfireCacheKey()")
    public Mono<UpstreamResult> fetchWildfires(BiosphereQuery query) {
        long startedAt = System.nanoTime();
        Geo.BBox bbox = configuredBounds();
        SourceMeta source = new SourceMeta("NASA FIRMS",
                "area/geojson latest",
                firmsClient.requestUrl(bbox, query.days()), "public-domain");

        return firmsClient.fetchActiveFires(bbox, query.days())
                .defaultIfEmpty(com.earthinformatics.explorer.util.JsonSupport.objectNode())
                .map(document -> buildWildfires(document, query, bbox))
                .map(payload -> UpstreamResult.success(payload, source, elapsed(startedAt)))
                .onErrorResume(error -> {
                    log.warn("Wildfire load failed: {}", error.toString());
                    return Mono.just(UpstreamResult.failure(
                            GeoJsonPayload.empty(Meta.live("NASA FIRMS")), source,
                            Failures.reason(error)));
                })
                .cache();
    }

    public Mono<GeoJsonPayload> wildfires(BiosphereQuery query) {
        return singleFlight.execute(query.wildfireCacheKey(), () -> fetchWildfires(query))
                .doOnNext(result -> cacheSupport.evictIfDegraded(Caches.WILDFIRES,
                        query.wildfireCacheKey(), result))
                .map(UpstreamResult::payload)
                .map(payload -> filterWildfires(payload, query));
    }

    private GeoJsonPayload buildWildfires(JsonNode document, BiosphereQuery query, Geo.BBox bbox) {
        List<JsonNode> features = new ArrayList<>();
        for (JsonNode feature : document.path("features")) {
            double[] position = GeoJson.position(feature);
            if (position == null || !Geo.isValidPosition(position[0], position[1])) {
                continue;
            }
            double frp = GeoJson.propertyAsDouble(feature, "frp", 0);
            int confidence = (int) GeoJson.propertyAsDouble(feature, "confidenceRank", 0);

            features.add(GeoJson.feature(
                    GeoJson.propertyAsString(feature, "id",
                            "fw-" + Math.round(position[1] * 100) + "-"
                                    + Math.round(position[0] * 100)),
                    position[0], position[1],
                    GeoJson.props(
                            "type", "wildfire",
                            "satellite", GeoJson.propertyAsString(feature, "satellite", "unknown"),
                            "confidence", GeoJson.propertyAsString(feature, "confidence", "unknown"),
                            "confidenceRank", confidence,
                            "frp", frp,
                            "frpUnit", "MW",
                            "brightness", GeoJson.propertyAsDouble(feature, "brightness", 0),
                            "brightnessUnit", "K",
                            "acquiredAt", GeoJson.propertyAsLong(feature, "acquiredAt", 0),
                            "acquisitionTime",
                            GeoJson.propertyAsString(feature, "acquisitionTime", null),
                            "heatClass", heatClass(frp),
                            "ageClass", ageClass(
                                    GeoJson.propertyAsLong(feature, "acquiredAt", 0)),
                            "source", "NASA FIRMS")));
        }

        Meta meta = Meta.live("NASA FIRMS")
                .with("days", query.days())
                .with("area", bbox.toString())
                .with("geometry", "passthrough")
                .with("units", Map.of("frp", "MW", "brightness", "K"))
                .with("legend", "heatClass: 0 none|1 low|2 moderate|3 high|4 extreme")
                .with("note", "DEMO_KEY restricts the queryable area; set EXPLORER_FIRMS_MAP_KEY "
                        + "for global coverage");
        return GeoJsonPayload.of(features, meta);
    }

    private GeoJsonPayload filterWildfires(GeoJsonPayload payload, BiosphereQuery query) {
        List<JsonNode> filtered = new ArrayList<>(payload.size());
        for (JsonNode feature : payload.features()) {
            if ((int) GeoJson.propertyAsDouble(feature, "confidenceRank", 0)
                    < query.minConfidence()) {
                continue;
            }
            double[] position = GeoJson.position(feature);
            if (position == null) {
                continue;
            }
            if (query.bbox() != null && !query.bbox().contains(position[0], position[1])) {
                continue;
            }
            filtered.add(feature);
        }
        // withFeatures last so featureCount describes the confidence- and viewport-filtered
        // result, not the full FIRMS response.
        return payload
                .withMeta(payload.meta()
                        .with("minConfidence", query.minConfidence())
                        .with("viewport", query.bbox() == null ? "global"
                                : query.bbox().toString()))
                .withFeatures(List.copyOf(filtered));
    }

    // ========================================================== vegetation

    @Cacheable(cacheNames = Caches.NDVI, key = "#query.vegetationCacheKey()")
    public Mono<UpstreamResult> fetchVegetation(BiosphereQuery query) {
        long startedAt = System.nanoTime();
        ExplorerProperties.Upstreams.Ndvi config = properties.upstreams().ndvi();
        SourceMeta source = new SourceMeta("NASA GIBS WMS", config.layerName(),
                config.baseUrl(), "varies-by-server");

        double halfCell = Math.max(0.5d, query.gridStep() / 2d);
        return ndviWmsClient.sampleNdvi(query.gridStep())
                .flatMap(sampling -> sampling.available()
                        ? Mono.just(UpstreamResult.success(
                                buildVegetation(sampling.samples(), halfCell, query.gridStep()),
                                source, elapsed(startedAt)))
                        : // The descriptor carries the time dimension, because a GetMap without a
                        // valid TIME returns a transparent tile and the layer silently renders as
                        // "no vegetation anywhere" - the exact failure this endpoint exists to
                        // avoid. The value is whatever the server advertised for this layer.
                        ndviWmsClient.probe().map(catalogue -> {
                            NdviWmsClient.WmsLayer layer =
                                    catalogue.layer(config.layerName());
                            String time = layer == null ? null : layer.defaultTime();
                            return UpstreamResult.failure(
                                    imageryDescriptor(sampling.reason(), query.gridStep(), time),
                                    source, sampling.reason());
                        }).defaultIfEmpty(UpstreamResult.failure(
                                imageryDescriptor(sampling.reason(), query.gridStep(), null),
                                source, sampling.reason())))
                .onErrorResume(error -> {
                    log.warn("Vegetation load failed: {}", error.toString());
                    return Mono.just(UpstreamResult.failure(
                            GeoJsonPayload.empty(Meta.live("NASA GIBS WMS")), source,
                            Failures.reason(error)));
                })
                .cache();
    }

    /**
     * Payload returned when the server cannot be sampled.
     *
     * <p>Rather than an empty collection the client receives the exact parameters needed to draw
     * the layer as a Cesium imagery provider, so the operator still gets live NDVI on the globe
     * and can see at a glance why no vectors were produced.
     */
    private GeoJsonPayload imageryDescriptor(String reason, double gridStep, String time) {
        ExplorerProperties.Upstreams.Ndvi config = properties.upstreams().ndvi();
        Meta meta = Meta.live("NASA GIBS WMS")
                .with("featureCount", 0)
                .with("gridStep", gridStep)
                .with("layer", config.layerName())
                .with("endpoint", config.baseUrl())
                .with("srs", config.srs())
                .with("style", config.style() == null ? "" : config.style())
                .with("renderAs", "imagery")
                .with("samplingSupported", false)
                // GIBS advertises the layer's time default as a bare date ("2026-08-29"), which is
                // the form its GetMap expects in TIME. An empty string is published rather than
                // omitted so the frontend never has to guess whether the key was missing or empty.
                .with("time", time == null ? "" : time)
                .with("version", "1.3.0")
                .with("note", "add this layer to Cesium as a WebMapServiceImageryProvider "
                        + "using the endpoint, layer, srs and time given here")
                .with("degraded", Boolean.TRUE);
        return GeoJsonPayload.empty(meta);
    }

    public Mono<GeoJsonPayload> vegetation(BiosphereQuery query) {
        return singleFlight.execute(query.vegetationCacheKey(), () -> fetchVegetation(query))
                .doOnNext(result -> cacheSupport.evictIfDegraded(Caches.NDVI,
                        query.vegetationCacheKey(), result))
                .map(UpstreamResult::payload)
                .map(payload -> {
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
                    return payload.withFeatures(List.copyOf(visible));
                });
    }

    private GeoJsonPayload buildVegetation(List<NdviSample> samples, double halfCell, double step) {
        List<JsonNode> features = new ArrayList<>(samples.size());
        for (NdviSample sample : samples) {
            if (!Geo.isValidPosition(sample.longitude(), sample.latitude())) {
                continue;
            }
            int vegetationClass = vegetationClass(sample.value());
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("ndvi", round(sample.value()));

            features.add(GeoJson.feature(
                    String.format(Locale.ROOT, "ndvi-%.2f-%.2f", sample.latitude(),
                            sample.longitude()),
                    GeoJson.cellPolygon(sample.longitude(), sample.latitude(), halfCell),
                    GeoJson.props(
                            "type", "vegetation",
                            "ndvi", round(sample.value()),
                            "vegetationClass", vegetationClass,
                            "vegetationLabel", vegetationLabel(vegetationClass),
                            "gridStep", step,
                            "detail", detail,
                            "source", "NASA GIBS WMS")));
        }

        Meta meta = Meta.live("NASA GIBS WMS")
                .with("gridStep", step)
                .with("geometryNote", "vegetation features are cell polygons")
                .with("legend", "vegetationClass: 0 bare|1 sparse|2 moderate|3 dense")
                .with("layer", properties.upstreams().ndvi().layerName())
                .with("endpoint", properties.upstreams().ndvi().baseUrl());
        return GeoJsonPayload.of(features, meta);
    }

    /** Layer names published by the configured WMS, for operator discovery. */
    public Mono<List<String>> vegetationLayers() {
        return ndviWmsClient.capabilities();
    }

    // ============================================================== summary

    @Cacheable(cacheNames = Caches.SUMMARIES, key = "'biosphere'", cacheManager = "shortLivedCacheManager")
    public Mono<DomainSummary> summary() {
        BiosphereQuery query = new BiosphereQuery("wildfires", 0, 0,
                properties.upstreams().ndvi().gridStepDegrees(), null);
        return wildfires(query).map(payload -> {
            double hottest = 0;
            int highConfidence = 0;
            for (JsonNode feature : payload.features()) {
                hottest = Math.max(hottest, GeoJson.propertyAsDouble(feature, "frp", 0));
                if (GeoJson.propertyAsDouble(feature, "confidenceRank", 0) >= 3) {
                    highConfidence++;
                }
            }
            return new DomainSummary("biosphere", payload.size(), payload.meta().degraded(),
                    payload.meta().fetchedAt(), hottest,
                    String.format(Locale.ROOT, "%.1f MW", hottest),
                    Meta.metrics("highConfidenceDetections", highConfidence,
                            "satellites", List.of("VIIRS", "MODIS")),
                    payload.size() > 40
                            ? List.copyOf(payload.features().subList(0, 40))
                            : List.copyOf(payload.features()));
        }).onErrorResume(error -> {
                    log.warn("biosphere summary failed: {}", error.toString());
                    return Mono.just(DomainSummary.unavailable("biosphere",
                            Failures.reason(error)));
                });
    }

    // ============================================================== helpers

    /** Fire Radiative Power banding, which is a far better size driver than raw confidence. */
    static int heatClass(double frpMw) {
        if (frpMw < 1) {
            return 0;
        }
        if (frpMw < 10) {
            return 1;
        }
        if (frpMw < 50) {
            return 2;
        }
        if (frpMw < 200) {
            return 3;
        }
        return 4;
    }

    /** Hours since acquisition; the renderer pulses recent detections more strongly. */
    static int ageClass(long acquiredAtMillis) {
        if (acquiredAtMillis <= 0) {
            return 4;
        }
        long ageHours = (System.currentTimeMillis() - acquiredAtMillis) / 3_600_000L;
        if (ageHours <= 2) {
            return 0;
        }
        if (ageHours <= 6) {
            return 1;
        }
        if (ageHours <= 12) {
            return 2;
        }
        return ageHours <= 24 ? 3 : 4;
    }

    /**
     * Standard NDVI thresholds.
     *
     * <p>Negative values are water, 0-0.15 is bare soil, and the remaining bands track the
     * convention used by NASA GIMMS and EO_SELENE.
     */
    static int vegetationClass(double ndvi) {
        if (Double.isNaN(ndvi) || ndvi < 0.0) {
            return 0;
        }
        if (ndvi < 0.15) {
            return 1;
        }
        if (ndvi < 0.40) {
            return 2;
        }
        return 3;
    }

    static String vegetationLabel(int vegetationClass) {
        return switch (vegetationClass) {
            case 0 -> "water-or-bare";
            case 1 -> "sparse";
            case 2 -> "moderate";
            default -> "dense";
        };
    }

    /** Query window for FIRMS: configuration, clamped to the provider's accepted range. */
    private Geo.BBox configuredBounds() {
        ExplorerProperties.Upstreams.Firms config = properties.upstreams().firms();
        Geo.BBox requested = new Geo.BBox(config.minLongitude(), config.minLatitude(),
                config.maxLongitude(), config.maxLatitude());
        return requested.snapped();
    }

    private static Double round(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return null;
        }
        return Math.round(value * 1000d) / 1000d;
    }

    private static long elapsed(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1_000_000L;
    }
}
