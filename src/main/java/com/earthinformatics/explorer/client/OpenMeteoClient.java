package com.earthinformatics.explorer.client;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.Geo;
import com.earthinformatics.explorer.util.LandMask;
import com.earthinformatics.explorer.util.UpstreamRetry;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpStatusCode;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Open-Meteo - weather forecast, air quality and marine statistics.
 *
 * <p>Three datasets, three base URLs, one code path:
 * <ul>
 *   <li>{@code https://api.open-meteo.com/v1/forecast} - cloud cover, pressure, wind vectors</li>
 *   <li>{@code https://air-quality-api.open-meteo.com/v1/air-quality} - PM2.5, PM10, US/EU AQI</li>
 *   <li>{@code https://marine-api.open-meteo.com/v1/marine} - wave height, currents, SST</li>
 * </ul>
 *
 * <p>All three accept a <b>comma separated list of coordinates</b> in a single request, which is
 * what makes a global field affordable: a 15 degree lattice (360 cells) costs a handful of round
 * trips instead of 360. Chunks are issued order-preserving but concurrent, and the whole
 * sampling pass is a single cache entry.
 *
 * <p>No API key is required. Open-Meteo asks for a descriptive user agent and caps traffic at a
 * few thousand requests per day per IP, both of which the shared {@code WebClient} handles.
 */
@Component
@Slf4j
public class OpenMeteoClient {

    /** Logical dataset selector. */
    public enum Dataset {
        FORECAST("forecast"),
        AIR_QUALITY("air-quality"),
        MARINE("marine");

        private final String path;

        Dataset(String path) {
            this.path = path;
        }

        /** URL path segment this product is served under. */
        public String path() {
            return path;
        }

        /**
         * True when the product only resolves over water.
         *
         * <p>Drives the land-mask filter: sending an inland coordinate to the marine API fails
         * the entire batch, not just the offending point.
         */
        public boolean oceanOnly() {
            return this == MARINE;
        }
    }

    /**
     * Field lists. They live here, next to the client, so the API contract documented in the
     * README cannot silently drift away from what we actually request.
     */
    public static final List<String> FORECAST_FIELDS = List.of(
            "temperature_2m", "surface_pressure", "cloud_cover",
            "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
            "relative_humidity_2m");

    public static final List<String> AIR_QUALITY_FIELDS = List.of(
            "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide",
            "ozone", "dust", "us_aqi", "european_aqi");

    public static final List<String> MARINE_FIELDS = List.of(
            "wave_height", "wave_direction", "wave_period", "swell_wave_height",
            "ocean_current_velocity", "ocean_current_direction", "sea_surface_temperature");

    /** One sampled point of the field, plus the model's own observation timestamp. */
    public record Reading(int row, int column, double latitude, double longitude,
                          Map<String, Double> values, Instant observedAt) {
    }

    private final WebClient webClient;
    private final ExplorerProperties properties;

    public OpenMeteoClient(WebClient webClient, ExplorerProperties properties) {
        this.webClient = webClient;
        this.properties = properties;
    }

    /**
     * Samples the {@code current} block over a global lattice.
     *
     * <p>For ocean-only products the lattice is filtered against the bundled land mask first. That
     * is not an optimisation: Open-Meteo answers a batch containing a single inland coordinate
     * with HTTP 400 for the <em>whole batch</em>, so an unfiltered global grid would fail every
     * time. Air quality and forecast are modelled everywhere and are left unfiltered.
     *
     * @param dataset Which Open-Meteo product to query.
     * @param step    Lattice resolution in degrees, clamped to the configured bounds.
     * @return readings in lattice order.
     */
    public Mono<GridSample> sampleGrid(Dataset dataset, double step) {
        ExplorerProperties.Upstreams.OpenMeteo config = properties.upstreams().openMeteo();
        if (!config.enabled()) {
            return Mono.just(new GridSample(List.of(), 0, 0, 0));
        }
        double effectiveStep = Math.max(config.minGridStepDegrees(),
                Math.min(step, config.maxGridStepDegrees()));

        LandMask.Partition partition = LandMask.partition(Geo.grid(effectiveStep));
        // Ocean-only products need the land mask. Everything else is modelled on land too and
        // must be sampled on the *whole* lattice - using partition.ocean() here silently
        // dropped every land cell from the wind and air-quality layers, which is most of the
        // populated planet and the entire point of those layers.
        List<Geo.GridCell> cells = dataset.oceanOnly() ? withinCoverage(partition) : allOf(partition);
        if (dataset.oceanOnly() && cells.size() < partition.total()) {
            log.debug("{}: sampling {} cells of {} ({} inland, {} outside model coverage)",
                    dataset, cells.size(), partition.total(), partition.land().size(),
                    partition.total() - partition.land().size() - cells.size());
        }

        List<List<Geo.GridCell>> chunks = Geo.chunk(cells, config.maxCellsPerChunk());
        List<String> fields = fieldsFor(dataset);
        java.util.concurrent.atomic.AtomicInteger failures =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger dropped =
                new java.util.concurrent.atomic.AtomicInteger();

        return Flux.fromIterable(chunks)
                // flatMapSequential keeps lattice order (so cells still line up with the rows and
                // columns advertised in the payload meta) while overlapping the requests.
                .flatMapSequential(chunk -> fetchChunkResilient(dataset, fields, chunk, 0)
                        .map(readings -> {
                            // A chunk that resolved to no readings is a *drop*, and only a drop
                            // if the request itself succeeded: conflating the two is how a
                            // 200-with-nulls outage came to be reported as a healthy empty layer.
                            boolean noData = readings.isEmpty();
                            if (noData) {
                                dropped.incrementAndGet();
                            }
                            return new ChunkResult(readings, false, noData);
                        })
                        .onErrorResume(error -> {
                            failures.incrementAndGet();
                            // Logged here rather than in fetchChunk, because this is the layer
                            // that knows a single failed chunk must not blank the whole field.
                            log.warn("Open-Meteo {} chunk of {} cells failed: {}", dataset,
                                    chunk.size(), error.toString());
                            return Mono.just(new ChunkResult(List.of(), true, false));
                        }), 2, 1)
                .concatMap(result -> Flux.fromIterable(result.readings()))
                .collectList()
                .map(readings -> new GridSample(readings, chunks.size(), failures.get(),
                        dropped.get()))
                .doOnNext(sample -> {
                    if (sample.failedChunks() > 0) {
                        log.warn("Open-Meteo {}: {} of {} lattice chunks failed", dataset,
                                sample.failedChunks(), sample.chunks());
                    }
                    if (sample.droppedChunks() > 0) {
                        log.warn("Open-Meteo {}: {} of {} lattice chunks resolved to no data",
                                dataset, sample.droppedChunks(), sample.chunks());
                    }
                    log.debug("Open-Meteo {} sampled {} readings from {} cells", dataset,
                            sample.readings().size(), cells.size());
                });
    }

    /**
     * Outcome of one chunk request, so that a partially failed lattice is distinguishable from a
     * lattice that legitimately had nothing to report.
     */
    private record ChunkResult(List<Reading> readings, boolean failed, boolean dropped) {
    }

    /**
     * Result of a lattice sampling run.
     *
     * @param readings      Every reading resolved across all chunks.
     * @param chunks        Number of lattice chunks the run was split into.
     * @param failedChunks  Chunks the provider rejected outright (rate limit, 5xx, bad request).
     * @param droppedChunks Chunks that resolved to nothing because the model has no data for
     *                      those cells. Legitimate, but an entirely empty result built this way
     *                      is indistinguishable from an outage unless it is reported.
     */
    public record GridSample(List<Reading> readings, int chunks, int failedChunks,
            int droppedChunks) {

        public boolean complete() {
            return failedChunks == 0 && droppedChunks == 0;
        }

        /** True when nothing at all came back and at least one request failed or was dropped. */
        public boolean unusable() {
            return readings.isEmpty() && (failedChunks > 0 || droppedChunks > 0);
        }

        /**
         * True when readings arrived but the lattice has holes in it.
         *
         * <p>A partial lattice is still worth serving - twenty good chunks beat none - but it must
         * not be presented as a global field. The lattice is a grid rendered as a map, so a
         * missing chunk is a visible blank rectangle, and a payload that reports
         * {@code degraded=false} for 23 surviving chunks out of 87 tells the user their air quality
         * picture is complete when two thirds of it is missing.
         */
        public boolean partial() {
            return !readings.isEmpty() && !complete();
        }

        /** Human-readable summary of what is missing, for the degraded reason. */
        public String incompleteness() {
            if (complete()) {
                return null;
            }
            StringBuilder reason = new StringBuilder();
            if (failedChunks > 0) {
                reason.append(failedChunks).append(" of ").append(chunks)
                        .append(" lattice chunks were rejected by the provider");
            }
            if (droppedChunks > 0) {
                if (reason.length() > 0) {
                    reason.append(" and ");
                }
                reason.append(droppedChunks).append(" of ").append(chunks)
                        .append(" lattice chunks resolved to no model data");
            }
            // "0 of 4 chunks were rejected" is worse than silence: it tells the reader nothing
            // and invites them to wonder what the real problem was.
            return reason.append("; the returned grid has holes").toString();
        }
    }

    /**
     * Describes how much of the requested lattice was withheld because it sat on land, so the
     * service can put it in the payload metadata instead of the user wondering where the missing
     * grid squares went.
     */
    public java.util.Optional<String> describeLandSkipping(double step) {
        if (!LandMask.available()) {
            return java.util.Optional.empty();
        }
        return LandMask.partition(Geo.grid(step)).describe();
    }

    /**
     * Single point request, used by the coordinate inspector endpoint.
     *
     * <p>The inspector reports "nothing here" as an empty result rather than as a 500, so this
     * caller - not {@link #fetchChunk} - contains transport failures. Containing them lower down
     * would have made every throttled chunk indistinguishable from a chunk the model has no data
     * for, and the lattice outcome counters could never fire.
     */
    public Mono<Reading> samplePoint(Dataset dataset, double latitude, double longitude) {
        List<String> fetchFields = fieldsFor(dataset);
        Geo.GridCell single = new Geo.GridCell(0, 0, latitude, longitude);
        return fetchChunkResilient(dataset, fetchFields, List.of(single), 0)
                .map(readings -> readings.isEmpty() ? null : readings.get(0))
                .doOnNext(reading -> {
                    if (reading == null) {
                        log.warn("Open-Meteo {} returned no data for {},{}", dataset, latitude,
                                longitude);
                    }
                })
                .onErrorResume(error -> {
                    log.warn("Open-Meteo {} point request for {},{} failed: {}", dataset, latitude,
                            longitude, error.toString());
                    return Mono.empty();
                });
    }

    /**
     * Southern limit of the marine model.
     *
     * <p>Open-Meteo's wave and current grids do not extend to the Antarctic coast; a request
     * below this latitude is answered with "No data is available for this location", which for a
     * multi-cell request fails the whole batch. The land mask alone is not enough because those
     * cells are water, just outside the model's domain.
     */
    public static final double MIN_MARINE_LATITUDE = -80.0;
    public static final double MAX_MARINE_LATITUDE = 80.0;

    /**
     * Drops cells the model cannot resolve, so a batch never fails as a whole.
     *
     * <p>Two filters stack: the bundled land mask removes the ~37% of cells that are inland, and
     * this clamp removes the high-latitude cells outside the marine domain. Together they make
     * the ordinary case a clean pass; {@link #fetchChunkResilient} is the safety net for anything
     * the two filters miss, such as a partially-globally-covered sea.
     */
    private static List<Geo.GridCell> withinCoverage(LandMask.Partition partition) {
        List<Geo.GridCell> covered = new ArrayList<>(partition.ocean().size());
        for (Geo.GridCell cell : partition.ocean()) {
            if (cell.latitude() >= MIN_MARINE_LATITUDE && cell.latitude() <= MAX_MARINE_LATITUDE) {
                covered.add(cell);
            }
        }
        return covered;
    }

    /**
     * The full lattice, in lattice order, for products that are modelled everywhere.
     *
     * <p>Land and ocean cells are concatenated rather than interleaved, so a payload built from
     * this list has to key rows and columns off the cell coordinates rather than off list index.
     */
    private static List<Geo.GridCell> allOf(LandMask.Partition partition) {
        List<Geo.GridCell> all = new ArrayList<>(partition.total());
        all.addAll(partition.ocean());
        all.addAll(partition.land());
        return all;
    }

    /**
     * Fetches a chunk, splitting it if the model rejects some of its cells.
     *
     * <p>With the land mask and the latitude clamp in place this path should not run, but "should
     * not" is not a guarantee - coastal cells at 1-degree mask resolution can still land on a
     * model domain edge, and a single such cell would otherwise blank 59 good readings. Splitting
     * is bounded by {@link #MAX_CHUNK_SPLITS} so a systematically unreachable chunk costs at most
     * a handful of requests and then gives up quietly.
     */
    private Mono<List<Reading>> fetchChunkResilient(Dataset dataset, List<String> fields,
            List<Geo.GridCell> cells, int depth) {
        return fetchChunk(dataset, fields, cells)
                .onErrorResume(NoDataException.class, error -> {
                    if (cells.size() <= 1 || depth >= MAX_CHUNK_SPLITS) {
                        log.debug("Dropping {} cell(s) the {} model cannot resolve, e.g. "
                                        + "{},{}", cells.size(), dataset,
                                cells.isEmpty() ? "n/a" : cells.get(0).latitude(),
                                cells.isEmpty() ? "n/a" : cells.get(0).longitude());
                        return Mono.just(List.<Reading>of());
                    }
                    int middle = cells.size() / 2;
                    List<Geo.GridCell> head = cells.subList(0, middle);
                    List<Geo.GridCell> tail = cells.subList(middle, cells.size());
                    return fetchChunkResilient(dataset, fields, head, depth + 1)
                            .zipWith(fetchChunkResilient(dataset, fields, tail, depth + 1))
                            .map(pair -> {
                                List<Reading> combined = new ArrayList<>(
                                        pair.getT1().size() + pair.getT2().size());
                                combined.addAll(pair.getT1());
                                combined.addAll(pair.getT2());
                                return combined;
                            });
                });
    }

    /**
     * Bounds recursive chunk splitting; see {@link #fetchChunkResilient}.
     *
     * <p>Kept deliberately small. Open-Meteo rejects a multi-coordinate request as a whole when a
     * single coordinate is out of model coverage, so the only way to keep the good cells is to
     * split. But every split is another billed request against an hourly quota, and a deep
     * binary split of a 60-cell chunk is up to 64 requests - which is how a lattice refresh can
     * exhaust the quota and starve every other dataset. Two levels find the offending cell
     * cheaply and abandon the rest of that chunk if it is still unreachable.
     */
    private static final int MAX_CHUNK_SPLITS = 2;

    /**
     * Signals that the model has no data for at least one coordinate in the batch, as opposed to a
     * transport or authorisation failure. Only this error is worth splitting a chunk for.
     */
    static final class NoDataException extends RuntimeException {

        NoDataException(String message) {
            super(message);
        }
    }

    /** A client error that will not be fixed by retrying or by splitting the batch. */
    static final class RequestRejectedException extends RuntimeException {

        RequestRejectedException(String message) {
            super(message);
        }
    }

    /**
     * Separates "the model has no data here" from "your request was wrong".
     *
     * <p>Both arrive as HTTP 400 with a JSON body. Only the first is recoverable by asking for
     * fewer coordinates, and conflating the two would turn a misconfigured field name into an
     * explosion of speculative requests.
     */
    private static RuntimeException classify(HttpStatusCode status, String body) {
        String text = body == null ? "" : body.strip();
        if (text.contains("No data is available")) {
            return new NoDataException(text);
        }
        String summary = text.length() > 300 ? text.substring(0, 300) + "..." : text;
        return new RequestRejectedException("Open-Meteo rejected the request with " + status
                + (summary.isEmpty() ? "" : ": " + summary));
    }

    private Mono<List<Reading>> fetchChunk(Dataset dataset, List<String> fields,
            List<Geo.GridCell> cells) {
        String latitudes = cells.stream()
                .map(cell -> coordinate(cell.latitude()))
                .collect(Collectors.joining(","));
        String longitudes = cells.stream()
                .map(cell -> coordinate(cell.longitude()))
                .collect(Collectors.joining(","));

        String url = baseUrl(dataset)
                + "?latitude=" + latitudes
                + "&longitude=" + longitudes
                + "&current=" + String.join(",", fields)
                + "&timezone=GMT"
                + "&forecast_days=1"
                + extraParams(dataset);

        // URI.create keeps the coordinate commas literal; percent-encoded commas (%2C) are
        // rejected by some Open-Meteo edge nodes.
        //
        // Every HTTP attempt passes through a shared permit gate: a cold cache refreshes several
        // datasets at once, and Open-Meteo answers the burst with HTTP 429 ("rejected lattice
        // chunks"). The gate is per attempt, so retries acquire a fresh permit instead of
        // re-firing into a still-saturated API.
        Mono<List<Reading>> attempt = Mono.defer(() ->
                        Mono.fromRunnable(() -> REQUEST_GATE.acquireUninterruptibly())
                                .subscribeOn(Schedulers.boundedElastic()))
                .then(webClient.get()
                        .uri(URI.create(url))
                        .retrieve()
                        // 4xx means the request itself is wrong and will not improve on retry, so it is
                        // mapped before the retry operator sees it. The "no data" case is separated out
                        // because it is the one 4xx that a smaller batch can fix.
                        .onStatus(HttpStatusCode::is4xxClientError, response -> response
                                .bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(body -> Mono.error(classify(response.statusCode(), body))))
                        .bodyToMono(JsonNode.class)
                        .map(body -> toReadings(body, cells))
                        .defaultIfEmpty(List.of()))
                .doFinally(signal -> REQUEST_GATE.release());

        return attempt
                .retryWhen(UpstreamRetry.backoff(
                        properties.resilience().retryBackoff(), properties.resilience().maxRetries()))
                .timeout(properties.resilience().responseTimeout())
                .onErrorResume(NoDataException.class, error -> Mono.error(error));
    }

    /**
     * Shared budget of concurrent Open-Meteo requests, so a cold-start refresh of every dataset
     * cannot trip the provider's per-IP limiter (which answers bursts with HTTP 429 and broken
     * lattice chunks). Deliberately far below any one layer's chunk count: the limiter cares
     * about how many requests are in flight at once, not how many a layer has queued.
     */
    private static final java.util.concurrent.Semaphore REQUEST_GATE =
            new java.util.concurrent.Semaphore(4, true);

    /** Open-Meteo answers a multi-location request with a JSON array, a single point with an object. */
    private List<Reading> toReadings(JsonNode body, List<Geo.GridCell> cells) {
        List<Reading> readings = new ArrayList<>(cells.size());
        List<JsonNode> documents = new ArrayList<>();
        if (body.isArray()) {
            body.forEach(documents::add);
        } else if (body.isObject()) {
            documents.add(body);
        }

        for (int index = 0; index < documents.size(); index++) {
            JsonNode document = documents.get(index);
            JsonNode current = document.path("current");
            if (!current.isObject()) {
                // Some cells come back as {"reason": "No data available"} - skip them.
                continue;
            }
            Map<String, Double> values = new LinkedHashMap<>();
            current.fields().forEachRemaining(entry -> {
                JsonNode value = entry.getValue();
                if (value.isNumber()) {
                    values.put(entry.getKey(), value.asDouble());
                }
            });
            if (values.isEmpty()) {
                // A 200 whose "current" block carries a timestamp and nothing else is Open-Meteo
                // saying "no data for this cell", not "a reading with no fields". Emitting it as
                // a Reading produced blank features in the payload, and - because the list was no
                // longer empty - it also defeated the chunk accounting, so a run where every
                // request came back null reported itself as a complete, healthy lattice.
                continue;
            }
            Geo.GridCell cell = index < cells.size() ? cells.get(index)
                    : new Geo.GridCell(0, index, document.path("latitude").asDouble(),
                    document.path("longitude").asDouble());
            readings.add(new Reading(cell.row(), cell.column(), cell.latitude(), cell.longitude(),
                    values, parseInstant(current.path("time").asText(null))));
        }
        return readings;
    }

    private static Instant parseInstant(String time) {
        if (time == null || time.isBlank()) {
            return null;
        }
        try {
            // Open-Meteo returns "2026-01-01T12:00" (no zone) when timezone=GMT.
            return java.time.LocalDateTime.parse(time).toInstant(java.time.ZoneOffset.UTC);
        } catch (RuntimeException malformed) {
            try {
                return Instant.parse(time);
            } catch (RuntimeException stillMalformed) {
                return null;
            }
        }
    }

    /**
     * Resolves the fully-qualified endpoint for a product.
     *
     * <p>Each Open-Meteo product lives at its own path below a shared host root
     * ({@code /v1/forecast}, {@code /v1/air-quality}, {@code /v1/marine}), so the configured base
     * is a root and the product segment has to be appended. Callers may also configure the full
     * path, in which case it is used verbatim - operators who front the API with a proxy
     * frequently need to, and silently double-appending the segment would 404.
     */
    private String baseUrl(Dataset dataset) {
        ExplorerProperties.Upstreams.OpenMeteo config = properties.upstreams().openMeteo();
        String root = switch (dataset) {
            case FORECAST -> config.forecastUrl();
            case AIR_QUALITY -> config.airQualityUrl();
            case MARINE -> config.marineUrl();
        };
        String segment = dataset.path();
        String trimmed = root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
        return trimmed.endsWith("/" + segment) ? trimmed : trimmed + "/" + segment;
    }

    private String extraParams(Dataset dataset) {
        return switch (dataset) {
            // Metres per second matches the wind vector geometry used by the renderer.
            case FORECAST -> "&wind_speed_unit=ms&temperature_unit=celsius";
            // "auto" resolves the regional AQI composite (US, European or CAMS) automatically.
            case AIR_QUALITY -> "&domains=auto";
            case MARINE -> "";
        };
    }

    private List<String> fieldsFor(Dataset dataset) {
        return switch (dataset) {
            case FORECAST -> FORECAST_FIELDS;
            case AIR_QUALITY -> AIR_QUALITY_FIELDS;
            case MARINE -> MARINE_FIELDS;
        };
    }

    /** Four decimal places is ~11 m, far finer than the model grid. */
    private static String coordinate(double value) {
        return String.format(Locale.ROOT, "%.4f", value);
    }
}
