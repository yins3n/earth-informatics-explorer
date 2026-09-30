package com.earthinformatics.explorer.service;

import com.earthinformatics.explorer.client.AisStreamClient;
import com.earthinformatics.explorer.client.AisStreamClient.VesselPosition;
import com.earthinformatics.explorer.client.OpenSkyClient;
import com.earthinformatics.explorer.config.CacheSupport;
import com.earthinformatics.explorer.config.Caches;
import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.Meta;
import com.earthinformatics.explorer.dto.SourceMeta;
import com.earthinformatics.explorer.dto.UpstreamResult;
import com.earthinformatics.explorer.dto.query.HumanImpactQuery;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.Geo;
import com.earthinformatics.explorer.util.Failures;
import com.earthinformatics.explorer.util.GeoJson;
import com.earthinformatics.explorer.util.JsonSupport;
import com.earthinformatics.explorer.util.SingleFlight;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Human-impact domain: live commercial aviation and maritime traffic.
 *
 * <p>Two sources with fundamentally different economics, which is why their cache policies
 * differ despite living in one service:
 * <ul>
 *   <li><b>OpenSky state vectors</b> - a polled REST resource. The payload is large (thousands of
 *       aircraft) but changes slowly enough for a 45 second TTL to be defensible, and that TTL is
 *       what keeps an anonymous deployment inside OpenSky's credit allowance.</li>
 *   <li><b>AISStream</b> - a pushed WebSocket owned by {@link AisStreamClient}. The "request"
 *       here is a read of an in-memory map, so the 30 second cache exists only to absorb
 *       dashboard polling storms, not to protect an upstream.</li>
 * </ul>
 */
@Service
@Slf4j
public class HumanImpactService {

    private final OpenSkyClient openSkyClient;
    private final AisStreamClient aisStreamClient;
    private final CacheSupport cacheSupport;
    private final SingleFlight singleFlight;
    private final ExplorerProperties properties;

    public HumanImpactService(OpenSkyClient openSkyClient, AisStreamClient aisStreamClient,
            CacheSupport cacheSupport, SingleFlight singleFlight, ExplorerProperties properties) {
        this.openSkyClient = openSkyClient;
        this.aisStreamClient = aisStreamClient;
        this.cacheSupport = cacheSupport;
        this.singleFlight = singleFlight;
        this.properties = properties;
    }

    // ================================================================ flights

    @Cacheable(cacheNames = Caches.FLIGHTS, key = "#query.flightsCacheKey()",
            cacheManager = "shortLivedCacheManager")
    public Mono<UpstreamResult> fetchFlights(HumanImpactQuery query) {
        long startedAt = System.nanoTime();
        SourceMeta source = new SourceMeta("OpenSky Network", "states/all",
                openSkyClient.statesUrl(), "CC-BY-SA-4.0 (non-commercial) / free for <400 req/day");

        Geo.BBox bbox = query.bbox();
        return openSkyClient.fetchStates(
                        bbox == null ? null : bbox.west(),
                        bbox == null ? null : bbox.south(),
                        bbox == null ? null : bbox.east(),
                        bbox == null ? null : bbox.north())
                .map(document -> buildFlights(document, query))
                .map(payload -> UpstreamResult.success(payload, source, elapsed(startedAt)))
                .onErrorResume(error -> {
                    log.warn("Flight load failed: {}", error.toString());
                    return Mono.just(UpstreamResult.failure(
                            GeoJsonPayload.empty(Meta.live("OpenSky Network")), source,
                            Failures.reason(error)));
                })
                .cache();
    }

    public Mono<GeoJsonPayload> flights(HumanImpactQuery query) {
        return singleFlight.execute(query.flightsCacheKey(), () -> fetchFlights(query))
                .doOnNext(result -> cacheSupport.evictIfDegraded(Caches.FLIGHTS,
                        query.flightsCacheKey(), result))
                .map(UpstreamResult::payload)
                .map(payload -> applyFilters(payload, query));
    }

    private GeoJsonPayload buildFlights(JsonNode document, HumanImpactQuery query) {
        List<JsonNode> features = new ArrayList<>();
        JsonNode states = document.path("states");
        if (states.isArray()) {
            for (JsonNode state : states) {
                if (!state.isArray()) {
                    continue;
                }
                double longitude = at(state, 5, Double.NaN);
                double latitude = at(state, 6, Double.NaN);
                if (!Geo.isValidPosition(longitude, latitude)) {
                    continue;
                }
                String icao24 = text(state, 0, "").trim().toLowerCase(Locale.ROOT);
                if (icao24.isEmpty()) {
                    continue;
                }
                boolean onGround = at(state, 8, 0) == 1;
                if (query.airborneOnly() && onGround) {
                    continue;
                }
                double baroAltitude = at(state, 7, Double.NaN);
                double geoAltitude = at(state, 13, Double.NaN);
                double altitude = Double.isNaN(geoAltitude) ? baroAltitude : geoAltitude;

                features.add(GeoJson.feature("ac-" + icao24, longitude, latitude, GeoJson.props(
                        "type", "aircraft",
                        "icao24", icao24,
                        "callsign", clean(text(state, 1, "")),
                        "originCountry", text(state, 2, ""),
                        "altitude", round(altitude),
                        "altitudeUnit", "m",
                        "altitudeType", Double.isNaN(geoAltitude) ? "barometric" : "geometric",
                        "velocity", round(at(state, 9, Double.NaN)),
                        "velocityUnit", "m/s",
                        "trueTrack", round(at(state, 10, Double.NaN)),
                        "verticalRate", round(at(state, 11, Double.NaN)),
                        "verticalRateUnit", "m/s",
                        "onGround", onGround,
                        "squawk", text(state, 14, ""),
                        "spi", spi(text(state, 15, "")),
                        "positionSource", positionSource(text(state, 16, "")),
                        "lastContact", at(state, 4, Double.NaN),
                        "flightClass", flightClass(altitude, at(state, 9, Double.NaN)),
                        "source", "OpenSky Network")));
            }
        }

        long observedAt = document.path("time").asLong(System.currentTimeMillis() / 1000);
        Meta meta = Meta.live("OpenSky Network")
                .with("observedAt", observedAt)
                .with("authenticated", openSkyClient.authenticated())
                .with("viewport", query.bbox() == null ? "global" : query.bbox().toString())
                .with("legend", "flightClass: 0 ground|1 low|2 mid|3 cruise")
                .with("units", Map.of("altitude", "m", "velocity", "m/s"))
                .with("note", "state vector coverage depends on receiver density; the ocean is "
                        + "sparsely covered by design");
        return GeoJsonPayload.of(features, meta);
    }

    private GeoJsonPayload applyFilters(GeoJsonPayload payload, HumanImpactQuery query) {
        List<JsonNode> filtered = new ArrayList<>(payload.size());
        for (JsonNode feature : payload.features()) {
            if (query.airborneOnly() && GeoJson.propertyAsBoolean(feature, "onGround", false)) {
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
        List<JsonNode> capped = filtered.size() > query.maxResults()
                ? filtered.subList(0, query.maxResults())
                : filtered;
        // withFeatures is deliberately last: it is what stamps featureCount from the collection
        // it actually holds. Applying withMeta afterwards would overwrite that with the
        // pre-cap meta and resurrect the "12221 declared, 8000 returned" mismatch.
        return payload
                .withMeta(payload.meta().with("matched", filtered.size())
                        .with("maxResults", query.maxResults())
                        .with("truncated", capped.size() < filtered.size()))
                .withFeatures(List.copyOf(capped));
    }

    // =============================================================== vessels

    /**
     * Maritime snapshot. Cached on the short tier purely to absorb dashboard polling; the
     * underlying data is pushed by AISStream and is always at most a few seconds old.
     */
    @Cacheable(cacheNames = Caches.VESSELS, key = "#query.vesselsCacheKey()",
            cacheManager = "shortLivedCacheManager")
    public Mono<UpstreamResult> fetchVessels(HumanImpactQuery query) {
        List<VesselPosition> snapshot = aisStreamClient.snapshot();
        SourceMeta source = new SourceMeta("AISStream.io", "VesselPositionReport",
                properties.upstreams().ais().url(), "AISStream terms of use");
        GeoJsonPayload payload = buildVessels(snapshot, query);

        if (!properties.upstreams().ais().enabled()) {
            return Mono.just(UpstreamResult.failure(payload, source,
                    "AISStream disabled by configuration"));
        }
        if (aisStreamClient.state() != AisStreamClient.ConnectionState.CONNECTED) {
            return Mono.just(UpstreamResult.failure(payload, source,
                    "AISStream connection state is " + aisStreamClient.state()));
        }
        return Mono.just(UpstreamResult.success(payload, source, 0L));
    }

    public Mono<GeoJsonPayload> vessels(HumanImpactQuery query) {
        return singleFlight.execute(query.vesselsCacheKey(), () -> fetchVessels(query))
                .map(UpstreamResult::payload);
    }

    private GeoJsonPayload buildVessels(List<VesselPosition> snapshot, HumanImpactQuery query) {
        List<JsonNode> features = new ArrayList<>(snapshot.size());
        for (VesselPosition vessel : snapshot) {
            if (query.bbox() != null && !query.bbox().contains(vessel.longitude(),
                    vessel.latitude())) {
                continue;
            }
            features.add(GeoJson.feature("mmsi-" + vessel.mmsi(), vessel.longitude(),
                    vessel.latitude(), GeoJson.props(
                            "type", "vessel",
                            "mmsi", vessel.mmsi(),
                            "name", vessel.name(),
                            "callSign", vessel.callSign(),
                            "shipType", vessel.shipType(),
                            "speed", round(vessel.speedKnots()),
                            "speedUnit", "kn",
                            "course", round(vessel.courseDegrees()),
                            "heading", round(vessel.headingDegrees()),
                            "navigationalStatus", vessel.navigationalStatus(),
                            "observedAt", vessel.timestamp().toString(),
                            "observedAtMillis", vessel.timestamp().toEpochMilli(),
                            "source", "AISStream.io")));
        }

        // The client already caps how many MMSI it tracks, but a viewport filter can only ever
        // shrink that set, so the query cap is applied here to keep the two layers symmetrical.
        List<JsonNode> capped = query.maxResults() > 0 && features.size() > query.maxResults()
                ? List.copyOf(features.subList(0, query.maxResults()))
                : features;

        Meta meta = Meta.live("AISStream.io")
                .with("maxResults", query.maxResults())
                .with("truncated", capped.size() < features.size())
                .with("connectionState", aisStreamClient.state().name())
                .with("messagesReceived", aisStreamClient.messagesReceived())
                .with("stalenessSeconds", aisStreamClient.staleness().toSeconds())
                .with("legend", "AIS navigational status codes: 0-15 per IMO 289 / ITF")
                .with("units", Map.of("speed", "knots", "course", "degrees true"));
        return GeoJsonPayload.of(capped, meta);
    }

    // ============================================================== summary

    @Cacheable(cacheNames = Caches.SUMMARIES, key = "'human-impact'", cacheManager = "shortLivedCacheManager")
    public Mono<DomainSummary> summary() {
        HumanImpactQuery query = new HumanImpactQuery(null, 8_000, false);
        return Mono.zip(flights(query), vessels(query))
                .map(tuple -> {
                    GeoJsonPayload flights = tuple.getT1();
                    GeoJsonPayload vessels = tuple.getT2();
                    return new DomainSummary("human-impact",
                            flights.size() + vessels.size(),
                            flights.meta().degraded() || vessels.meta().degraded(),
                            flights.meta().fetchedAt(),
                            flights.size(),
                            String.format(Locale.ROOT, "%d airborne", flights.size()),
                            Meta.metrics("aircraft", flights.size(),
                                    "vessels", vessels.size(),
                                    "aisState", aisStreamClient.state().name(),
                                    "aisStalenessSeconds", aisStreamClient.staleness().toSeconds()),
                            List.of());
                })
                .onErrorResume(error -> {
                    log.warn("human-impact summary failed: {}", error.toString());
                    return Mono.just(DomainSummary.unavailable("human-impact",
                            Failures.reason(error)));
                });
    }

    // ============================================================== helpers

    private static double at(JsonNode state, int index, double fallback) {
        if (!state.isArray() || index >= state.size()) {
            return fallback;
        }
        JsonNode value = state.get(index);
        return value.isNumber() ? value.asDouble() : fallback;
    }

    private static String text(JsonNode state, int index, String fallback) {
        if (!state.isArray() || index >= state.size()) {
            return fallback;
        }
        JsonNode value = state.get(index);
        return value.isNull() ? fallback : value.asText(fallback);
    }

    /**
     * SPI (special purpose indicator) arrives as a JSON string nested inside the state array,
     * e.g. {@code "{\"value\":false,\"spi\":null}"}. Older deployments send a bare boolean.
     */
    private static boolean spi(String raw) {
        if (raw == null || raw.isBlank()) {
            return false;
        }
        String value = raw.strip();
        if (value.startsWith("{")) {
            try {
                return JsonSupport.mapper().readTree(value).path("value").asBoolean(false);
            } catch (Exception notJson) {
                return false;
            }
        }
        return "true".equalsIgnoreCase(value);
    }

    private static String clean(String value) {
        if (value == null) {
            return "";
        }
        String trimmed = value.strip();
        return trimmed.isEmpty() || "@@@".equals(trimmed) ? "" : trimmed;
    }

    /** Vertical band used for the aircraft icon scale. */
    static int flightClass(double altitudeMetres, double velocityMs) {
        if (Double.isNaN(altitudeMetres) || altitudeMetres < 100) {
            return 0;
        }
        if (altitudeMetres < 3_000) {
            return 1;
        }
        if (altitudeMetres < 9_000) {
            return 2;
        }
        return 3;
    }

    private static String positionSource(String code) {
        return switch (code == null ? "" : code) {
            case "0" -> "ADS-B";
            case "1" -> "ASTERIX";
            case "2" -> "MLAT";
            case "3" -> "FLARM";
            default -> "unknown";
        };
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
