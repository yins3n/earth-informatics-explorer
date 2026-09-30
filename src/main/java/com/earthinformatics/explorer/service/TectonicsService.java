package com.earthinformatics.explorer.service;

import com.earthinformatics.explorer.client.EonetClient;
import com.earthinformatics.explorer.client.UsgsClient;
import com.earthinformatics.explorer.config.CacheSupport;
import com.earthinformatics.explorer.config.Caches;
import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.Meta;
import com.earthinformatics.explorer.dto.SourceMeta;
import com.earthinformatics.explorer.dto.UpstreamResult;
import com.earthinformatics.explorer.dto.query.TectonicsQuery;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.Geo;
import com.earthinformatics.explorer.util.Failures;
import com.earthinformatics.explorer.util.GeoJson;
import com.earthinformatics.explorer.util.JsonSupport;
import com.earthinformatics.explorer.util.SingleFlight;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tectonic domain: earthquakes (USGS) and active volcanoes (NASA EONET).
 *
 * <p>Both providers publish GeoJSON, so this service is mostly a <em>pass-through with
 * normalisation</em>:
 * <ol>
 *   <li>Fetch one or more upstream feeds concurrently (never sequentially - USGS publishes
 *       several feeds covering different magnitude/time windows).</li>
 *   <li>Augment each feature with renderer-facing fields: a normalised {@code magnitude}, a
 *       magnitude bucket for the legend, a depth class, an epoch-millis {@code eventTime} and a
 *       stable {@code id}. Everything the provider sent is preserved.</li>
 *   <li>Sort by event time descending so the most recent events render first.</li>
 * </ol>
 *
 * <p>Caching strategy: the cached value is the <em>global</em> document. Viewport, magnitude
 * threshold and result cap are applied afterwards, so an unbounded number of distinct client
 * viewports still results in exactly one upstream poll per TTL.
 */
@Service
@Slf4j
public class TectonicsService {

    /** Depth classes used by the renderer legend (km below the surface). */
    private static final double SHALLOW_KM = 70;
    private static final double INTERMEDIATE_KM = 300;

    private final UsgsClient usgsClient;
    private final EonetClient eonetClient;
    private final CacheSupport cacheSupport;
    private final SingleFlight singleFlight;
    private final ExplorerProperties properties;

    public TectonicsService(UsgsClient usgsClient, EonetClient eonetClient, CacheSupport cacheSupport,
            SingleFlight singleFlight, ExplorerProperties properties) {
        this.usgsClient = usgsClient;
        this.eonetClient = eonetClient;
        this.cacheSupport = cacheSupport;
        this.singleFlight = singleFlight;
        this.properties = properties;
    }

    // =========================================================== earthquakes

    /**
     * Cached, global earthquake document.
     *
     * <p>The pipeline ends with {@link Mono#cache()} on purpose: it makes the cached entry
     * <em>replayable</em>, so a second request served from Caffeine replays the already-fetched
     * document instead of re-subscribing to the (cold) HTTP publisher.
     */
    @Cacheable(cacheNames = Caches.EARTHQUAKES, key = "#query.cacheKey()")
    public Mono<UpstreamResult> fetchEarthquakes(TectonicsQuery query) {
        long startedAt = System.nanoTime();
        List<String> feeds = resolveFeeds(query.feed());

        return Flux.fromIterable(feeds)
                // concatMap keeps the merge order stable across refreshes; each feed is a single
                // small request, so serialising them costs nothing and keeps the token bucket calm.
                .concatMap(feed -> usgsClient.fetchFeed(feed)
                        .onErrorResume(error -> {
                            log.warn("USGS feed {} unavailable: {}", feed, error.toString());
                            return Mono.just(JsonSupport.objectNode());
                        }))
                // ArrayNode is itself an Iterable<JsonNode>, so this flattens every feed into
                // one stream while preserving feed order.
                .flatMapIterable(node -> node.path("features"))
                .filter(JsonNode::isObject)
                .collectList()
                .map(features -> normaliseEarthquakes(features, feeds))
                .map(payload -> UpstreamResult.success(payload,
                        provenance(feeds), elapsedMillis(startedAt)))
                .onErrorResume(error -> {
                    log.warn("Earthquake load failed: {}", error.toString());
                    return Mono.just(UpstreamResult.failure(
                            GeoJsonPayload.empty(Meta.live("USGS")), provenance(feeds),
                            Failures.reason(error)));
                })
                .cache();
    }

    /** Public entry point: cached fetch, then per-viewport shaping. */
    public Mono<GeoJsonPayload> earthquakes(TectonicsQuery query) {
        return singleFlight.execute(query.cacheKey(), () -> fetchEarthquakes(query))
                .doOnNext(result -> cacheSupport.evictIfDegraded(Caches.EARTHQUAKES,
                        query.cacheKey(), result))
                .map(UpstreamResult::payload)
                .map(payload -> shape(payload, query));
    }

    private GeoJsonPayload shape(GeoJsonPayload payload, TectonicsQuery query) {
        List<JsonNode> filtered = new ArrayList<>();
        for (JsonNode feature : payload.features()) {
            double[] position = GeoJson.position(feature);
            if (position == null) {
                continue;
            }
            if (query.hasViewport() && !query.bbox().contains(position[0], position[1])) {
                continue;
            }
            if (GeoJson.propertyAsDouble(feature, "magnitude", 0) < query.minMagnitude()) {
                continue;
            }
            filtered.add(feature);
        }
        List<JsonNode> capped = filtered.size() > query.maxResults()
                ? filtered.subList(0, query.maxResults())
                : filtered;

        Meta meta = payload.meta()
                .with("returned", capped.size())
                .with("matched", filtered.size())
                .with("minMagnitude", query.minMagnitude())
                .with("viewport", query.bbox() == null ? "global" : query.bbox().toString())
                .with("legend", "magnitudeClass: 0 lt2.5|1 2.5-4|2 4-6|3 6-7|4 gt7");
        return GeoJsonPayload.of(List.copyOf(capped), meta);
    }

    /**
     * Augments upstream features. Pass-through is intentional: we never rebuild the geometry, and
     * we never drop a provider property - a future USGS field appears in our API for free.
     */
    private GeoJsonPayload normaliseEarthquakes(List<JsonNode> upstream, List<String> feeds) {
        List<JsonNode> features = new ArrayList<>(upstream.size());
        Set<String> seenIds = new LinkedHashSet<>();

        for (JsonNode feature : upstream) {
            double[] position = GeoJson.position(feature);
            if (position == null || !Geo.isValidPosition(position[0], position[1])) {
                continue;
            }
            String id = feature.path("id").asText(null);
            if (id == null || !seenIds.add(id)) {
                // Merging several USGS feeds legitimately yields duplicates (all_day contains
                // events that also appear in all_hour); keep the newest of each.
                continue;
            }
            double magnitude = GeoJson.propertyAsDouble(feature, "mag", 0);
            double depth = GeoJson.propertyAsDouble(feature, "depth", 0);
            long eventTime = GeoJson.propertyAsLong(feature, "time", 0);

            JsonNode augmented = GeoJson.augment(feature, id, GeoJson.props(
                    "type", "earthquake",
                    "magnitude", round1(magnitude),
                    "magnitudeClass", magnitudeClass(magnitude),
                    "depth", depth,
                    "depthClass", depthClass(depth),
                    "depthKm", Math.abs(round1(depth)),
                    "place", GeoJson.propertyAsString(feature, "place", "unknown"),
                    "eventTime", eventTime,
                    "felt", GeoJson.propertyAsLong(feature, "felt", 0),
                    "alert", GeoJson.propertyAsString(feature, "alert", null),
                    "tsunami", GeoJson.propertyAsBoolean(feature, "tsunami", false),
                    "significance", GeoJson.propertyAsLong(feature, "cdi", 0),
                    "source", "USGS",
                    "sourceDataset", String.join("+", feeds),
                    "url", GeoJson.propertyAsString(feature, "url", null)));

            if (augmented != null) {
                features.add(augmented);
            }
        }

        features.sort(Comparator.comparingLong(
                feature -> -GeoJson.propertyAsLong(feature, "eventTime", 0)));

        Meta meta = Meta.live("USGS")
                .with("feeds", List.copyOf(feeds))
                .with("normalised", true)
                .with("geometry", "passthrough");
        return GeoJsonPayload.of(features, meta);
    }

    /** {@code 0} .. {@code 4}, matching the five-step colour ramp in the front-end legend. */
    static int magnitudeClass(double magnitude) {
        if (magnitude < 2.5) {
            return 0;
        }
        if (magnitude < 4.0) {
            return 1;
        }
        if (magnitude < 6.0) {
            return 2;
        }
        if (magnitude < 7.0) {
            return 3;
        }
        return 4;
    }

    /** Standard seismological depth banding used by every USGS product. */
    static String depthClass(double depthKm) {
        double depth = Math.abs(depthKm);
        if (depth <= SHALLOW_KM) {
            return "shallow";
        }
        return depth <= INTERMEDIATE_KM ? "intermediate" : "deep";
    }

    // ============================================================= volcanoes

    @Cacheable(cacheNames = Caches.VOLCANOES, key = "#query.volcanoCacheKey()")
    public Mono<UpstreamResult> fetchVolcanoes(TectonicsQuery query) {
        long startedAt = System.nanoTime();
        SourceMeta source = new SourceMeta("NASA EONET",
                query.category(), eonetClient.eventsUrl(query.category()), "public-domain");

        return eonetClient.fetchEvents(query.category(), query.days(), query.maxResults())
                .map(document -> normaliseVolcanoes(document, query))
                .map(payload -> UpstreamResult.success(payload, source, elapsedMillis(startedAt)))
                .onErrorResume(error -> {
                    log.warn("Volcano load failed: {}", error.toString());
                    return Mono.just(UpstreamResult.failure(
                            GeoJsonPayload.empty(Meta.live("NASA EONET")), source,
                            Failures.reason(error)));
                })
                .cache();
    }

    public Mono<GeoJsonPayload> volcanoes(TectonicsQuery query) {
        return singleFlight.execute(query.volcanoCacheKey(), () -> fetchVolcanoes(query))
                .doOnNext(result -> cacheSupport.evictIfDegraded(Caches.VOLCANOES,
                        query.volcanoCacheKey(), result))
                .map(UpstreamResult::payload)
                .map(payload -> {
                    if (!query.hasViewport()) {
                        return payload;
                    }
                    List<JsonNode> visible = new ArrayList<>();
                    for (JsonNode feature : payload.features()) {
                        double[] position = centroid(feature);
                        if (position != null && query.bbox().contains(position[0], position[1])) {
                            visible.add(feature);
                        }
                    }
                    return payload.withFeatures(List.copyOf(visible));
                });
    }

    /**
     * EONET events carry either a Point or a Polygon geometry; the renderer needs a representative
     * position for filtering and labelling.
     */
    private double[] centroid(JsonNode feature) {
        JsonNode geometry = feature.path("geometry");
        if ("Point".equals(geometry.path("type").asText())) {
            return GeoJson.position(feature);
        }
        JsonNode coordinates = geometry.path("coordinates");
        if (coordinates.isArray() && coordinates.size() > 0) {
            JsonNode ring = coordinates.get(0);
            if (ring.isArray() && ring.size() > 0) {
                JsonNode first = ring.get(0);
                return new double[]{first.asDouble(), first.asDouble(1)};
            }
        }
        return null;
    }

    /**
     * Converts EONET v3 events into GeoJSON features.
     *
     * <p>EONET v3 dropped the {@code properties} member and the aviation {@code alertLevel} that
     * the v1 API published; both are reconstructed here from what v3 actually returns:
     * <ul>
     *   <li>name - the event {@code title} ("Nevados del Chillan Volcano, Chile");</li>
     *   <li>position and observation date - the <em>most recent</em> entry of the event's
     *       {@code geometry} array, which EONET orders oldest first;</li>
     *   <li>provenance - the {@code categories} and {@code sources} arrays, the latter pointing
     *       at the Smithsonian Global Volcanism Program record.</li>
     * </ul>
     * The geometry is rebuilt as a clean RFC 7946 object: EONET stores {@code magnitudeValue},
     * {@code magnitudeUnit} and {@code date} as siblings of {@code type}/{@code coordinates},
     * and passing that node through would put non-standard members inside {@code geometry}.
     */
    private GeoJsonPayload normaliseVolcanoes(JsonNode document, TectonicsQuery query) {
        List<JsonNode> features = new ArrayList<>();
        JsonNode events = document.path("events");
        if (events.isArray()) {
            for (JsonNode event : events) {
                JsonNode geometry = latestGeometry(event.path("geometry"));
                if (geometry == null) {
                    continue;
                }
                String id = event.path("id").asText("evt-" + features.size());
                String type = geometry.path("type").asText("Point");
                JsonNode coordinates = geometry.path("coordinates");
                if (!coordinates.isArray() || coordinates.size() < 2) {
                    continue;
                }

                double[] position = representativePoint(type, coordinates);
                if (position == null) {
                    continue;
                }
                double[] positionNode = {position[0], position[1]};
                JsonNode geometryNode = "Point".equals(type)
                        ? GeoJson.point(positionNode[0], positionNode[1])
                        : GeoJson.geometry(type, coordinates);

                // EONET publishes no magnitude for most events. Omitting the key rather than
                // sending 0.0 keeps "not reported" distinguishable from a real magnitude 0 -
                // otherwise every volcano renders as a magnitude-zero dot in a magnitude legend.
                Double magnitude = geometry.path("magnitudeValue").isNumber()
                        ? geometry.path("magnitudeValue").asDouble()
                        : null;
                long observedAt = parseTimestamp(geometry.path("date").asText(null));
                boolean open = event.path("closed").isMissingNode()
                        || event.path("closed").isNull();

                Map<String, Object> extras = new LinkedHashMap<>();
                extras.put("type", "volcano");
                extras.put("name", event.path("title").asText("unnamed"));
                extras.put("categoryTitles", categoryTitles(event.path("categories")));
                if (magnitude != null) {
                    extras.put("magnitude", round1(magnitude));
                    extras.put("magnitudeClass", magnitudeClass(magnitude));
                }
                extras.put("observedAt", observedAt);
                extras.put("open", open);
                extras.put("source", "NASA EONET");
                extras.put("eventLink", event.path("link").asText(null));
                extras.put("sourceIds", sourceIds(event.path("sources")));
                extras.put("sourceUrl", firstSourceUrl(event.path("sources")));
                extras.put("position", positionNode[0] + "," + positionNode[1]);

                JsonNode augmented = GeoJson.augment(
                        GeoJson.feature(id, geometryNode, new LinkedHashMap<>()), id, extras);
                if (augmented != null) {
                    features.add(augmented);
                }
            }
        }

        Meta meta = Meta.live("NASA EONET")
                .with("category", query.category())
                .with("days", query.days())
                .with("source", "EONET v3 / Smithsonian Global Volcanism Program")
                .with("note", "EONET v3 no longer publishes an aviation alert level; "
                        + "magnitude is the reported volcanic magnitude when present")
                .with("legend", "magnitudeClass: 0 <2.5|1 <4|2 <6|3 <7|4 >=7");
        return GeoJsonPayload.of(features, meta);
    }

    /**
     * Most recent entry of an EONET {@code geometry} array. EONET lists a track oldest-first, so
     * the last entry is the current position rather than the first sighting of the eruption.
     */
    private static JsonNode latestGeometry(JsonNode holder) {
        if (holder == null || holder.isNull() || holder.isMissingNode()) {
            return null;
        }
        if (!holder.isArray()) {
            return holder;
        }
        JsonNode latest = null;
        for (JsonNode candidate : holder) {
            if (candidate == null || candidate.isNull()) {
                continue;
            }
            if (latest == null
                    || candidate.path("date").asText("").compareTo(
                            latest.path("date").asText("")) >= 0) {
                latest = candidate;
            }
        }
        return latest;
    }

    /** A position usable for labelling and viewport filtering, for Point or polygon geometries. */
    private static double[] representativePoint(String type, JsonNode coordinates) {
        if ("Point".equals(type)) {
            return new double[]{coordinates.get(0).asDouble(), coordinates.get(1).asDouble()};
        }
        JsonNode ring = coordinates.isArray() && coordinates.size() > 0
                ? coordinates.get(0)
                : null;
        if (ring == null || !ring.isArray() || ring.size() < 2) {
            return null;
        }
        JsonNode first = ring.get(0);
        return new double[]{first.asDouble(), first.asDouble(1)};
    }

    private static List<String> categoryTitles(JsonNode categories) {
        List<String> titles = new ArrayList<>();
        if (categories.isArray()) {
            for (JsonNode category : categories) {
                String title = category.path("title").asText(null);
                if (title != null && !title.isBlank() && !titles.contains(title)) {
                    titles.add(title);
                }
            }
        }
        return List.copyOf(titles);
    }

    private static List<String> sourceIds(JsonNode sources) {
        List<String> ids = new ArrayList<>();
        if (sources.isArray()) {
            for (JsonNode source : sources) {
                String sourceId = source.path("id").asText(null);
                if (sourceId != null && !sourceId.isBlank() && !ids.contains(sourceId)) {
                    ids.add(sourceId);
                }
            }
        }
        return List.copyOf(ids);
    }

    private static String firstSourceUrl(JsonNode sources) {
        return sources.isArray() && sources.size() > 0
                ? sources.get(0).path("url").asText(null)
                : null;
    }


    private static long parseTimestamp(String iso) {
        if (iso == null || iso.isBlank()) {
            return System.currentTimeMillis();
        }
        try {
            return java.time.Instant.parse(iso).toEpochMilli();
        } catch (RuntimeException malformed) {
            try {
                return java.time.LocalDateTime.parse(iso)
                        .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            } catch (RuntimeException stillMalformed) {
                return System.currentTimeMillis();
            }
        }
    }

    // ============================================================== summary

    /**
     * Pre-aggregated roll-up for the WebSocket tick. Cached separately (short TTL) because it
     * is recomputed from the payload on every tick.
     */
    @Cacheable(cacheNames = Caches.SUMMARIES, key = "'tectonics'", cacheManager = "shortLivedCacheManager")
    public Mono<DomainSummary> summary() {
        return earthquakes(new TectonicsQuery("auto", "volcanoes", 30, 0, 2_000, null))
                .map(earthquakes -> {
                    List<JsonNode> features = earthquakes.features();
                    double maxMagnitude = 0;
                    int highestClass = 0;
                    for (JsonNode feature : features) {
                        maxMagnitude = Math.max(maxMagnitude,
                                GeoJson.propertyAsDouble(feature, "magnitude", 0));
                        highestClass = Math.max(highestClass,
                                (int) GeoJson.propertyAsDouble(feature, "magnitudeClass", 0));
                    }
                    List<JsonNode> latest = features.size() > 20
                            ? features.subList(0, 20)
                            : features;
                    return new DomainSummary("tectonics", features.size(), earthquakes.meta().degraded(),
                            earthquakes.meta().fetchedAt(), maxMagnitude,
                            String.format(Locale.ROOT, "M%.1f", maxMagnitude),
                            Meta.metrics("highestClass", highestClass,
                                    "magnitudeUnit", "moment magnitude"),
                            List.copyOf(latest));
                })
                .onErrorResume(error -> {
                    log.warn("tectonics summary failed: {}", error.toString());
                    return Mono.just(DomainSummary.unavailable("tectonics",
                            Failures.reason(error)));
                });
    }

    // ============================================================== helpers

    private List<String> resolveFeeds(String requested) {
        ExplorerProperties.Upstreams.Usgs config = properties.upstreams().usgs();
        if (requested == null || requested.equals("auto")) {
            String configured = config.defaultFeed();
            return configured != null && !configured.isBlank()
                    ? List.of(configured)
                    : List.copyOf(config.feeds());
        }
        return config.feeds().contains(requested) ? List.of(requested) : List.of(config.defaultFeed());
    }

    private SourceMeta provenance(List<String> feeds) {
        return new SourceMeta("USGS", String.join(",", feeds),
                usgsClient.feedUrl(feeds.get(0)), "public-domain");
    }

    private static long elapsedMillis(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1_000_000L;
    }

    private static double round1(double value) {
        return Math.round(value * 10d) / 10d;
    }
}
