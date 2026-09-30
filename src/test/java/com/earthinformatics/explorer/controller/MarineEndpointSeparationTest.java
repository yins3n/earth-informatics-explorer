package com.earthinformatics.explorer.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.earthinformatics.explorer.TestProperties;
import com.earthinformatics.explorer.client.NoaaCoOpsClient;
import com.earthinformatics.explorer.client.OpenMeteoClient;
import com.earthinformatics.explorer.config.CacheConfig;
import com.earthinformatics.explorer.config.CacheSupport;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.query.OceansQuery;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.service.OceansService;
import com.earthinformatics.explorer.util.SingleFlight;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * The marine upstream returns sea-surface temperature and currents in a single response, so one
 * fetch serves both. Both endpoints used to return that whole mixed payload verbatim, which meant
 * {@code /currents} and {@code /sea-surface-temperature} were byte-identical and the dashboard
 * could not tell the two layers apart: it filtered the currents layer on the declared feature
 * type, found SST cells mixed in, and refused to draw the layer at all.
 *
 * <p>Each endpoint must now return only the feature types it declares.
 */
class MarineEndpointSeparationTest {

    private MockWebServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    /**
     * Echoes the cells the client asked for, one document per cell, each carrying a "current"
     * block with both variables. The client parses an array of per-cell objects (or a single
     * object), not Open-Meteo's time-series shape, so the mock has to match what the parser
     * reads or every reading is silently skipped.
     */
    private void echoCellsWithBothVariables() {
        server.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                Map<String, String> query = queryParams(request);
                List<Double> latitudes = numbers(query.get("latitude"));
                List<Double> longitudes = numbers(query.get("longitude"));
                int count = Math.min(latitudes.size(), longitudes.size());
                StringBuilder body = new StringBuilder("[");
                for (int i = 0; i < count; i++) {
                    if (i > 0) {
                        body.append(",");
                    }
                    body.append("{\"latitude\":").append(latitudes.get(i))
                        .append(",\"longitude\":").append(longitudes.get(i))
                        .append(",\"current\":{\"time\":\"2026-09-29T00:00\"")
                        .append(",\"sea_surface_temperature\":").append(12.5 + i)
                        .append(",\"ocean_current_velocity\":0.8")
                        .append(",\"ocean_current_direction\":200.0")
                        .append("}}");
                }
                body.append("]");
                return new MockResponse()
                        .setResponseCode(HttpStatus.OK.value())
                        .setHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                        .setBody(body.toString());
            }
        });
    }

    private static Map<String, String> queryParams(RecordedRequest request) {
        Map<String, String> parsed = new java.util.LinkedHashMap<>();
        String path = request.getPath();
        int mark = path == null ? -1 : path.indexOf('?');
        if (mark < 0) {
            return parsed;
        }
        for (String pair : path.substring(mark + 1).split("&")) {
            int equals = pair.indexOf('=');
            if (equals > 0) {
                parsed.put(pair.substring(0, equals), pair.substring(equals + 1));
            }
        }
        return parsed;
    }

    private static List<Double> numbers(String csv) {
        List<Double> values = new java.util.ArrayList<>();
        if (csv == null || csv.isBlank()) {
            return values;
        }
        for (String part : csv.split(",")) {
            values.add(Double.parseDouble(part));
        }
        return values;
    }

    private OceansService service() {
        ExplorerProperties properties = TestProperties.builder()
                .openMeteo(server.url("/forecast").toString(),
                        server.url("/air-quality").toString(),
                        server.url("/marine").toString())
                .noaa(server.url("/coops").toString())
                .fast()
                .build();
        WebClient webClient = WebClient.builder().build();
        CacheConfig config = new CacheConfig();
        return new OceansService(
                new NoaaCoOpsClient(webClient, properties),
                new OpenMeteoClient(webClient, properties),
                new CacheSupport(config.cacheManager(properties), config.shortLivedCacheManager(properties)),
                new SingleFlight(),
                properties);
    }

    private static Set<String> typesOf(GeoJsonPayload payload) {
        Set<String> types = new TreeSet<>();
        for (JsonNode feature : payload.features()) {
            JsonNode type = feature.path("properties").path("type");
            if (type.isTextual()) {
                types.add(type.asText());
            }
        }
        return types;
    }

    @Test
    @DisplayName("the currents endpoint returns currents and nothing else")
    void currentsOnly() {
        echoCellsWithBothVariables();
        OceansQuery query = new OceansQuery("currents", 15, null);

        GeoJsonPayload payload = service().currentsAndTemperature(
                query, Set.of("ocean-current")).block();

        assertThat(payload).isNotNull();
        assertThat(typesOf(payload)).containsExactly("ocean-current");
        assertThat(payload.features()).isNotEmpty();
    }

    @Test
    @DisplayName("the sea-surface-temperature endpoint returns temperature and nothing else")
    void temperatureOnly() {
        echoCellsWithBothVariables();
        OceansQuery query = new OceansQuery("sst", 15, null);

        GeoJsonPayload payload = service().currentsAndTemperature(
                query, Set.of("sea-surface-temperature")).block();

        assertThat(payload).isNotNull();
        assertThat(typesOf(payload)).containsExactly("sea-surface-temperature");
    }

    @Test
    @DisplayName("no filter keeps every type, for the combined domain endpoint")
    void noFilterKeepsEverything() {
        echoCellsWithBothVariables();
        OceansQuery query = new OceansQuery("all", 15, null);

        GeoJsonPayload payload = service().currentsAndTemperature(query).block();

        assertThat(payload).isNotNull();
        assertThat(typesOf(payload))
                .containsExactly("ocean-current", "sea-surface-temperature");
    }

    @Test
    @DisplayName("a filtered payload reports a feature count that matches what it carries")
    void filteredPayloadKeepsCountHonest() {
        echoCellsWithBothVariables();
        OceansQuery query = new OceansQuery("currents", 15, null);

        GeoJsonPayload payload = service().currentsAndTemperature(
                query, Set.of("ocean-current")).block();

        assertThat(payload).isNotNull();
        assertThat(payload.meta().details().get("featureCount"))
                .as("the count must describe the filtered payload, not the pre-filter total")
                .isEqualTo(payload.features().size());
    }
}
