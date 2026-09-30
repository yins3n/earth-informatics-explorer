package com.earthinformatics.explorer.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.earthinformatics.explorer.TestProperties;
import com.earthinformatics.explorer.dto.TideStation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.test.StepVerifier;

/**
 * NOAA CO-OPS response handling.
 *
 * <p>CO-OPS is inconsistent in two ways that both presented as "the endpoint returns nothing":
 * it names the result array after the product for some product/date combinations and {@code "data"}
 * for others, and it uses {@code v}/{@code t} in current rows where older deployments used
 * {@code v_wtmp}/{@code v_date}. It also reports a soft failure as HTTP 200 with an
 * {@code error} member, which is a success status code carrying a failure.
 */
class NoaaCoOpsClientTest {

    private MockWebServer server;
    private NoaaCoOpsClient client;
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        mapper = new ObjectMapper();
        client = new NoaaCoOpsClient(WebClient.builder().build(),
                TestProperties.builder()
                        .noaa(server.url("/datagetter").toString())
                        .fast()
                        .build());
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    private void enqueue(String body) {
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .setBody(body));
    }

    @Nested
    @DisplayName("result-array key")
    class ResultKey {

        @Test
        @DisplayName("accepts the product-named array")
        void acceptsProductKey() throws Exception {
            assertThat(NoaaCoOpsClient.hasPayload(mapper.readTree("""
                    {"metadata":{"id":"8518750"},"predictions":[{"v":"1.2","type":"H"}]}
                    """), "predictions")).isTrue();
        }

        @Test
        @DisplayName("accepts a data array, which is what water_temperature returns")
        void acceptsDataKey() throws Exception {
            // The bug this pins: only the product key was accepted, so every water-temperature
            // station was rejected as malformed and the layer reported zero readings as healthy.
            assertThat(NoaaCoOpsClient.hasPayload(mapper.readTree("""
                    {"metadata":{"id":"8518750"},"data":[{"t":"2026-09-28 00:00","v":"19.2"}]}
                    """), "water_temperature")).isTrue();
        }

        @Test
        @DisplayName("rejects a body with no result array at all")
        void rejectsEmptyBody() throws Exception {
            assertThat(NoaaCoOpsClient.hasPayload(
                    mapper.readTree("{\"metadata\":{\"id\":\"8518750\"}}"),
                    "water_temperature")).isFalse();
        }
    }

    @Nested
    @DisplayName("water temperature")
    class WaterTemperature {

        @Test
        @DisplayName("parses the current v/t row shape")
        void parsesCurrentShape() {
            enqueue("""
                    {"metadata":{"id":"8518750","name":"The Battery","lat":"40.7006","lon":"-74.0142"},
                     "data":[{"t":"2026-09-28 00:00","v":"19.2","f":"0,0,0"},
                             {"t":"2026-09-28 01:00","v":"19.3","f":"0,0,0"}]}
                    """);

            StepVerifier.create(client.waterTemperature("8518750"))
                    .assertNext(body -> {
                        assertThat(body.path("data")).hasSize(2);
                        assertThat(body.path("data").get(0).path("v").asText()).isEqualTo("19.2");
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("turns a soft failure into a real error so the station is recorded")
        void softFailureIsAnError() {
            enqueue("""
                    {"error":{"message":"No data was found. This product may not be offered at this station at the requested time."}}
                    """);

            StepVerifier.create(client.waterTemperature("8443970"))
                    .expectError(NoaaCoOpsClient.UpstreamException.class)
                    .verify();
        }

        @Test
        @DisplayName("requests the product with a bounded date range and metric units")
        void requestShape() throws InterruptedException {
            enqueue("{\"data\":[]}");

            StepVerifier.create(client.waterTemperature("8518750")).expectNextCount(1)
                    .verifyComplete();

            String path = server.takeRequest().getPath();
            assertThat(path)
                    .contains("product=water_temperature")
                    .contains("station=8518750")
                    .contains("units=metric")
                    .contains("time_zone=gmt")
                    .contains("format=json")
                    .contains("begin_date=")
                    .contains("end_date=");
        }
    }

    @Nested
    @DisplayName("predictions")
    class Predictions {

        @Test
        @DisplayName("keeps the datum and interval a tide curve needs")
        void requestShape() throws InterruptedException {
            enqueue("{\"predictions\":[]}");

            StepVerifier.create(client.predictions("8518750", "hilo", 2)).expectNextCount(1)
                    .verifyComplete();

            String path = server.takeRequest().getPath();
            assertThat(path)
                    .contains("product=predictions")
                    .contains("interval=hilo")
                    .contains("datum=MSL")
                    .contains("units=metric");
        }

        @Test
        @DisplayName("surfaces an invalid station as an UpstreamException naming the product")
        void invalidStation() {
            enqueue("""
                    {"error":{"message":"No Predictions data was found. Please make sure the Datum input is valid."}}
                    """);

            StepVerifier.create(client.predictions("0000000", "hilo", 2))
                    .expectErrorSatisfies(error -> {
                        assertThat(error).isInstanceOf(NoaaCoOpsClient.UpstreamException.class);
                        assertThat(error.getMessage()).contains("predictions");
                    })
                    .verify();
        }
    }

    @Test
    @DisplayName("the built-in station set is not empty and has usable coordinates")
    void stationSet() {
        List<TideStation> stations = client.stations();

        assertThat(stations).isNotEmpty();
        assertThat(stations).allSatisfy(station -> {
            assertThat(station.id()).isNotBlank();
            assertThat(station.latitude()).isBetween(-90d, 90d);
            assertThat(station.longitude()).isBetween(-180d, 180d);
        });
        assertThat(stations.stream().map(TideStation::id).distinct().count())
                .as("station ids are unique")
                .isEqualTo(stations.size());
    }

    @Test
    @DisplayName("a JSON body is parsed, not stringified")
    void bodyIsStructured() {
        enqueue("{\"data\":[{\"t\":\"2026-09-28 00:00\",\"v\":\"19.2\"}]}");

        StepVerifier.create(client.waterTemperature("8518750"))
                .assertNext((JsonNode body) -> assertThat(body.isObject()).isTrue())
                .verifyComplete();
    }
}
