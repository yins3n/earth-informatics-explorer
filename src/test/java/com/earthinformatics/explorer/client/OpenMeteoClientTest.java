package com.earthinformatics.explorer.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.earthinformatics.explorer.TestProperties;
import com.earthinformatics.explorer.util.LandMask;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.test.StepVerifier;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.Dispatcher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Open-Meteo lattice sampling.
 *
 * <p>Two separate failures lived here, and both produced the same symptom: a payload with zero
 * features and {@code degraded=false}.
 *
 * <p>The first was a counting bug. A chunk that returned HTTP 200 with no values was flagged as
 * "dropped" in a record whose flag was then thrown away, so {@code droppedChunks} was permanently
 * zero, {@code complete()} was permanently true, and a run where every single request had been
 * thinned to nothing reported itself as a clean sweep.
 *
 * <p>The second was a cell-selection bug where non-ocean products sampled {@code ocean()} instead
 * of the whole lattice, so the wind and air-quality layers quietly omitted every land cell.
 */
class OpenMeteoClientTest {

    private MockWebServer server;
    private OpenMeteoClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        client = new OpenMeteoClient(WebClient.builder().build(),
                TestProperties.builder()
                        .openMeteo(server.url("/forecast").toString(),
                                server.url("/air-quality").toString(),
                                server.url("/marine").toString())
                        .fast()
                        .build());
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    /** A 200 whose {@code current} block is structurally present but empty. */
    private void enqueueEmptyCurrent() {
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .setBody("""
                        {"latitude":0,"longitude":0,"generationtime_ms":0.1,
                         "utc_offset_seconds":0,"timezone":"GMT",
                         "current_units":{"time":"iso8601","wind_speed_10m":"m/s"},
                         "current":{"time":"2026-09-11T12:00"}}
                        """));
    }

    private void enqueueRateLimited() {
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.TOO_MANY_REQUESTS.value())
                .setHeader("Content-Type", MediaType.TEXT_PLAIN_VALUE)
                .setBody("Hourly API request limit exceeded. Please try again in the next hour."));
    }

    @Test
    @DisplayName("a chunk that succeeds with no values counts as dropped, not as complete")
    void emptyChunksAreDropped() {
        // Enough responses for every chunk at the configured resolution, all of them empty.
        int chunks = 40;
        for (int i = 0; i < chunks; i++) {
            enqueueEmptyCurrent();
        }

        StepVerifier.create(client.sampleGrid(OpenMeteoClient.Dataset.FORECAST, 90))
                .assertNext(sample -> {
                    assertThat(sample.readings()).isEmpty();
                    assertThat(sample.droppedChunks())
                            .as("every empty-but-successful chunk is a drop")
                            .isEqualTo(sample.chunks());
                    assertThat(sample.failedChunks()).isZero();
                    assertThat(sample.complete()).as("an all-dropped run is not complete").isFalse();
                    assertThat(sample.unusable())
                            .as("all chunks empty is unusable, not a healthy empty layer")
                            .isTrue();
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("a rejected chunk counts as failed, distinctly from a drop")
    void rejectedChunksAreFailures() {
        for (int i = 0; i < 40; i++) {
            enqueueRateLimited();
        }

        StepVerifier.create(client.sampleGrid(OpenMeteoClient.Dataset.FORECAST, 90))
                .assertNext(sample -> {
                    assertThat(sample.failedChunks()).isPositive();
                    assertThat(sample.droppedChunks())
                            .as("a 429 is a failure, not a model with no data")
                            .isZero();
                    assertThat(sample.unusable()).isTrue();
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("a partially successful lattice reports drops and failures separately")
    void partialLatticeReportsBothCounts() {
        // Two chunks (9 lattice cells at a 90 degree step, capped at 5 per chunk): the first is
        // served an empty 200, the second is throttled twice. Deterministic, because a 429 retry
        // consumes the next queued response - which is exactly why an alternating
        // empty/throttled/throttled fixture cannot be reasoned about.
        OpenMeteoClient twoChunks = new OpenMeteoClient(WebClient.builder().build(),
                TestProperties.builder()
                        .openMeteo(server.url("/forecast").toString(),
                                server.url("/air-quality").toString(),
                                server.url("/marine").toString())
                        .openMeteoMaxCells(5)
                        .fast()
                        .build());

        enqueueEmptyCurrent();
        enqueueRateLimited();
        enqueueRateLimited();

        StepVerifier.create(twoChunks.sampleGrid(OpenMeteoClient.Dataset.FORECAST, 90))
                .assertNext(sample -> {
                    assertThat(sample.chunks()).isEqualTo(2);
                    assertThat(sample.failedChunks()).as("the throttled chunk").isEqualTo(1);
                    assertThat(sample.droppedChunks()).as("the empty-but-200 chunk").isEqualTo(1);
                    assertThat(sample.complete()).isFalse();
                    assertThat(sample.unusable())
                            .as("no readings survived either chunk")
                            .isTrue();
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("GridSample reports an outcome the service can act on")
    void gridSampleOutcomeContract() {
        OpenMeteoClient.GridSample healthy = new OpenMeteoClient.GridSample(
                List.of(new OpenMeteoClient.Reading(0, 0, 0, 0, Map.of("t", 1.0), null)),
                2, 0, 0);
        OpenMeteoClient.GridSample thinned = new OpenMeteoClient.GridSample(
                List.of(new OpenMeteoClient.Reading(0, 0, 0, 0, Map.of("t", 1.0), null)),
                4, 0, 3);
        OpenMeteoClient.GridSample dead = new OpenMeteoClient.GridSample(
                List.of(), 4, 2, 2);
        OpenMeteoClient.GridSample genuinelyEmpty = new OpenMeteoClient.GridSample(
                List.of(), 0, 0, 0);

        assertThat(healthy.complete()).isTrue();
        assertThat(healthy.unusable()).isFalse();

        assertThat(thinned.complete()).as("data withheld by the model is not complete").isFalse();
        assertThat(thinned.unusable()).as("some data survived").isFalse();
        assertThat(thinned.partial()).as("a holey grid is partial").isTrue();
        assertThat(thinned.incompleteness())
                .as("only the drops are mentioned, never a hollow '0 of 4 rejected'")
                .isEqualTo("3 of 4 lattice chunks resolved to no model data; the returned grid "
                        + "has holes");

        OpenMeteoClient.GridSample throttled = new OpenMeteoClient.GridSample(
                List.of(new OpenMeteoClient.Reading(0, 0, 0, 0, Map.of("t", 1.0), null)),
                4, 2, 0);
        assertThat(throttled.incompleteness())
                .isEqualTo("2 of 4 lattice chunks were rejected by the provider; the returned grid "
                        + "has holes");

        assertThat(healthy.partial()).isFalse();
        assertThat(healthy.incompleteness()).isNull();

        assertThat(dead.partial()).as("no readings at all is unusable, not partial").isFalse();

        assertThat(dead.unusable()).isTrue();

        assertThat(genuinelyEmpty.unusable())
                .as("nothing was asked for, so nothing can have failed")
                .isFalse();
    }

    @Test
    @DisplayName("sampling a disabled provider makes no request and reports nothing failed")
    void disabledProviderIsQuiet() {
        OpenMeteoClient disabled = new OpenMeteoClient(WebClient.builder().build(),
                TestProperties.builder()
                        .openMeteoEnabled(false)
                        .fast()
                        .build());

        StepVerifier.create(disabled.sampleGrid(OpenMeteoClient.Dataset.FORECAST, 30))
                .assertNext(sample -> {
                    assertThat(sample.chunks()).isZero();
                    assertThat(sample.unusable()).isFalse();
                })
                .verifyComplete();

        assertThat(server.getRequestCount()).isZero();
    }

    @Test
    @DisplayName("the land mask still gates ocean-only products")
    void oceanOnlyProductsUseTheLandMask() throws InterruptedException {
        for (int i = 0; i < 40; i++) {
            enqueueEmptyCurrent();
        }

        StepVerifier.create(client.sampleGrid(OpenMeteoClient.Dataset.MARINE, 90))
                .assertNext(sample -> assertThat(sample.chunks()).isPositive())
                .verifyComplete();

        // Coordinates travel in the query string, not a body. Sampling an inland cell makes
        // Open-Meteo reject the whole batch, so this is a correctness property, not an
        // optimisation: the requested cells must be water within the marine latitude band.
        String query = server.takeRequest().getPath();
        List<Double> latitudes = numbersAfter(query, "latitude=");
        List<Double> longitudes = numbersAfter(query, "longitude=");

        assertThat(latitudes).isNotEmpty();
        assertThat(latitudes).allSatisfy(latitude -> {
            assertThat(latitude).isGreaterThanOrEqualTo(OpenMeteoClient.MIN_MARINE_LATITUDE);
            assertThat(latitude).isLessThanOrEqualTo(OpenMeteoClient.MAX_MARINE_LATITUDE);
        });
        assertThat(longitudes).isNotEmpty();
        for (int index = 0; index < latitudes.size(); index++) {
            assertThat(LandMask.isOcean(longitudes.get(index), latitudes.get(index)))
                    .as("cell %s,%s is over land", longitudes.get(index), latitudes.get(index))
                    .isTrue();
        }
    }

    @Test
    @DisplayName("the partition knows which cells are land")
    void partitionSplitsTheGlobe() {
        LandMask.Partition partition = LandMask.partition(
                com.earthinformatics.explorer.util.Geo.grid(30));

        assertThat(partition.total()).isPositive();
        assertThat(partition.land()).as("there is land on Earth").isNotEmpty();
        assertThat(partition.ocean()).as("there is ocean on Earth").isNotEmpty();
        assertThat(partition.describe()).isPresent();
    }

    /** Parses the comma-separated coordinate list that follows {@code key} in a query string. */
    private static List<Double> numbersAfter(String query, String key) {
        int start = query.indexOf(key);
        if (start < 0) {
            return List.of();
        }
        String rest = query.substring(start + key.length());
        int end = rest.indexOf('&');
        List<Double> values = new ArrayList<>();
        for (String token : (end < 0 ? rest : rest.substring(0, end)).split(",")) {
            values.add(Double.parseDouble(token));
        }
        return values;
    }
    @Test
    @DisplayName("a burst of simultaneous lattices is serialized through the shared request gate")
    void burstIsGated() {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        AtomicBoolean seen = new AtomicBoolean();
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        server.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(okhttp3.mockwebserver.RecordedRequest request) {
                int now = inFlight.incrementAndGet();
                maxObserved.accumulateAndGet(now, Math::max);
                if (now > 1) {
                    seen.set(true);
                }
                firstEntered.countDown();
                // Hold the first request open so the rest of the burst must queue on the gate.
                try {
                    if (now <= 1) {
                        release.await(10, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                Map<String, String> query = params(request.getPath());
                List<Double> lats = csv(query.get("latitude"));
                List<Double> lons = csv(query.get("longitude"));
                int count = Math.min(lats.size(), lons.size());
                StringBuilder body = new StringBuilder("[");
                for (int i = 0; i < count; i++) {
                    if (i > 0) {
                        body.append(",");
                    }
                    body.append("{\"latitude\":").append(lats.get(i)).append(',')
                        .append("\"longitude\":").append(lons.get(i)).append(',')
                        .append("\"current\":{\"time\":\"2026-09-29T00:00\",")
                        .append("\"wind_speed_10m\":3.1,\"cloud_cover\":5,")
                        .append("\"pressure_msl\":1012.0}}");
                }
                body.append("]");
                inFlight.decrementAndGet();
                return new MockResponse().setResponseCode(HttpStatus.OK.value())
                        .setHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .setBody(body.toString());
            }
        });

        reactor.core.Disposable subscription = Flux.range(0, 12)
                .flatMap(index -> client.sampleGrid(OpenMeteoClient.Dataset.FORECAST, 90)
                        .subscribeOn(Schedulers.parallel()))
                .subscribeOn(Schedulers.parallel())
                .subscribe();
        try {
            assertThat(firstEntered.await(10, TimeUnit.SECONDS))
                    .as("first chunk arrives")
                    .isTrue();
            Thread.sleep(300);
            release.countDown();
            assertThat(subscription).isNotNull();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        int observed = maxObserved.get();
        assertThat(observed)
                .as("more than one request queued behind the first, proving the burst")
                .isGreaterThanOrEqualTo(2);
        assertThat(observed)
                .as("the gate never lets more than 4 Open-Meteo requests be in flight at once")
                .isLessThanOrEqualTo(4);
    }

    private static Map<String, String> params(String path) {
        Map<String, String> parsed = new java.util.LinkedHashMap<>();
        if (path == null) return parsed;
        int mark = path.indexOf('?');
        if (mark < 0) return parsed;
        for (String pair : path.substring(mark + 1).split("&")) {
            int equals = pair.indexOf('=');
            if (equals > 0) parsed.put(pair.substring(0, equals), pair.substring(equals + 1));
        }
        return parsed;
    }

    private static List<Double> csv(String raw) {
        List<Double> values = new ArrayList<>();
        if (raw == null || raw.isBlank()) return values;
        for (String token : raw.split(",")) {
            values.add(Double.parseDouble(token));
        }
        return values;
    }
}
