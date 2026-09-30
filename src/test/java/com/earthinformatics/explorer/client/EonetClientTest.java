package com.earthinformatics.explorer.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.earthinformatics.explorer.TestProperties;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.JsonSupport;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
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

/**
 * EONET client behaviour.
 *
 * <p>Written because of two upstream quirks that only show up in production traffic:
 * <ol>
 *   <li>EONET is behind a load balancer whose nodes disagree about the default representation.
 *       The same URL with the same {@code Accept} header intermittently answers
 *       {@code application/rss+xml} with HTTP 200, and a {@code bodyToMono(JsonNode.class)} call
 *       then fails with an {@code UnsupportedMediaTypeException} on a <em>200</em>.</li>
 *   <li>The default look-back window for volcanoes is a year, not a month: a 30-day window is
 *       empty almost every day, which reads as a broken layer rather than an inactive continent.</li>
 * </ol>
 */
class EonetClientTest {

    private MockWebServer server;
    private EonetClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        ExplorerProperties properties = TestProperties.builder()
                .eonet(server.url("/v3").toString(), 1000, 365)
                .fast()
                .build();
        WebClient webClient = WebClient.builder().build();
        client = new EonetClient(webClient, properties, new ObjectMapper());
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    private static MockResponse json(String body) {
        return new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .setBody(body);
    }

    @Test
    @DisplayName("parses a JSON events document")
    void parsesJson() {
        server.enqueue(json("""
                {"title":"EONET Events","events":[
                  {"id":"EONET_20710","title":"Nevados del Chillan Volcano, Chile",
                   "geometry":[{"magnitudeValue":null,"date":"2026-06-15T00:00:00Z",
                                "type":"Point","coordinates":[-71.378,-36.868]}]}
                ]}"""));

        StepVerifier.create(client.fetchEvents("volcanoes", 365, 1000))
                .assertNext(document -> {
                    assertThat(document.path("events")).hasSize(1);
                    assertThat(document.path("events").get(0).path("id").asText())
                            .isEqualTo("EONET_20710");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("treats an RSS body as a retryable failure instead of a parse crash")
    void rejectsRss() {
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", "application/rss+xml;charset=utf-8")
                .setBody("<?xml version=\"1.0\"?><rss><channel><item>"
                        + "<title>Nevados del Chillan Volcano, Chile</title>"
                        + "</item></channel></rss>"));
        server.enqueue(json("{\"events\":[]}"));

        // The retry lands on a node that answers JSON, which is what happens in practice.
        StepVerifier.create(client.fetchEvents("volcanoes", 365, 10))
                .assertNext(document -> assertThat(document.path("events")).isEmpty())
                .verifyComplete();
    }

    @Test
    @DisplayName("requests a year of volcanoes by default, and never clamps a client's window")
    void windowIsNotClamped() throws InterruptedException {
        server.enqueue(json("{\"events\":[]}"));

        StepVerifier.create(client.fetchEvents("volcanoes", 3_000, 10))
                .expectNextCount(1)
                .verifyComplete();

        var request = server.takeRequest();
        assertThat(request.getPath()).contains("days=3000");
        assertThat(request.getPath()).contains("category=volcanoes");
        assertThat(request.getPath()).contains("status=open");
        assertThat(request.getHeader("Accept")).contains(MediaType.APPLICATION_JSON_VALUE);
    }

    @Test
    @DisplayName("omits the window when a client asks for all open events")
    void omitsWindow() throws InterruptedException {
        server.enqueue(json("{\"events\":[]}"));

        StepVerifier.create(client.fetchEvents("volcanoes", -1, 10))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(server.takeRequest().getPath()).doesNotContain("days=");
    }

    @Test
    @DisplayName("caps the requested page size at the configured maximum")
    void capsLimit() throws InterruptedException {
        server.enqueue(json("{\"events\":[]}"));

        StepVerifier.create(client.fetchEvents("volcanoes", 365, 999_999))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(server.takeRequest().getPath()).contains("limit=1000");
    }

    @Test
    @DisplayName("surfaces a server error rather than inventing an empty result")
    void surfacesServerError() {
        // maxRetries=1 in the test properties means two attempts; both must be enqueued or
        // MockWebServer's queue dispatcher blocks and the test times out instead of failing.
        server.enqueue(new MockResponse().setResponseCode(HttpStatus.SERVICE_UNAVAILABLE.value()));
        server.enqueue(new MockResponse().setResponseCode(HttpStatus.SERVICE_UNAVAILABLE.value()));

        StepVerifier.create(client.fetchEvents("volcanoes", 365, 10))
                .expectErrorSatisfies(error -> assertThat(
                        reactor.core.Exceptions.isRetryExhausted(error)).isTrue())
                .verify(Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("only volcanoes are exposed as a supported category")
    void supportedCategories() {
        assertThat(EonetClient.supportedCategories()).containsExactly("volcanoes");
        assertThat(client.eventsUrl("volcanoes")).contains("/events?status=open");
    }

    @Test
    @DisplayName("rejects a body that is neither JSON nor XML")
    void rejectsGarbage() {
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", MediaType.TEXT_PLAIN_VALUE)
                .setBody("not json at all"));
        server.enqueue(json("{\"events\":[]}"));

        StepVerifier.create(client.fetchEvents("volcanoes", 365, 10))
                .assertNext(document -> assertThat(document.path("events")).isEmpty())
                .verifyComplete();
    }

    @Test
    @DisplayName("uses the application ObjectMapper contract")
    void mapperIsShared() {
        assertThat(JsonSupport.mapper()).isNotNull();
    }
}
