package com.earthinformatics.explorer.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.earthinformatics.explorer.TestProperties;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.HostRateLimiterRegistry;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * The shared outbound client.
 *
 * <p>The {@code User-Agent} is not cosmetic. NASA GIBS and NOAA publish request policies and
 * throttle unidentified clients harder than identified ones, and a default Reactor Netty agent
 * is indistinguishable from a scraper. Several providers also publish per-agent rate limits, so
 * the header is part of the contract with the upstream, which is why it is asserted here rather
 * than left to a code review to notice.
 */
class WebClientConfigTest {

    private MockWebServer server;
    private WebClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        ExplorerProperties properties = TestProperties.builder().fast().build();
        client = new WebClientConfig().upstreamWebClient(properties,
                new HostRateLimiterRegistry(properties));
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    @DisplayName("every upstream request identifies itself")
    void sendsUserAgent() throws Exception {
        server.enqueue(json("{}"));

        StepVerifier.create(client.get().uri(server.url("/probe").toString()).retrieve().bodyToMono(String.class))
                .expectNext("{}")
                .verifyComplete();

        RecordedRequest recorded = server.takeRequest();
        assertThat(recorded.getHeader(HttpHeaders.USER_AGENT))
                .as("providers throttle unidentified clients")
                .isEqualTo(WebClientConfig.USER_AGENT);
        assertThat(WebClientConfig.USER_AGENT)
                .as("the agent must name the project so operators can be identified")
                .startsWith("EarthInformaticsExplorer/");
    }

    @Test
    @DisplayName("a per-request path does not lose the identifying headers")
    void keepsHeadersOnEveryRequest() throws Exception {
        server.enqueue(json("{\"ok\":true}"));
        server.enqueue(json("{\"ok\":true}"));

        // zip, not then: then() discards the first Mono's only value.
        StepVerifier.create(
                        Mono.zip(client.get().uri(server.url("/a").toString()).retrieve()
                                        .bodyToMono(String.class),
                                client.get().uri(server.url("/b").toString()).retrieve()
                                        .bodyToMono(String.class)))
                .assertNext(pair -> {
                    assertThat(pair.getT1()).isEqualTo("{\"ok\":true}");
                    assertThat(pair.getT2()).isEqualTo("{\"ok\":true}");
                })
                .verifyComplete();

        List<RecordedRequest> requests = List.of(server.takeRequest(), server.takeRequest());
        assertThat(requests).hasSize(2);
        assertThat(requests)
                .allSatisfy(request -> assertThat(request.getHeader(HttpHeaders.USER_AGENT))
                        .isEqualTo(WebClientConfig.USER_AGENT));
    }

    @Test
    @DisplayName("bursts are paced by the per-host token bucket rather than opening sockets at once")
    void rateLimitsPerHost() {
        int responses = 6;
        for (int i = 0; i < responses; i += 1) {
            server.enqueue(json("{}"));
        }
        // The default bucket is sized so this many immediate calls cannot all be released at
        // once; the filter defers the excess instead of bursting.
        StepVerifier.create(Mono.zip(client.get().uri(server.url("/burst").toString()).retrieve()
                                .bodyToMono(String.class),
                        client.get().uri(server.url("/burst").toString()).retrieve()
                                .bodyToMono(String.class)))
                .expectNextCount(1)
                .verifyComplete();

        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    private static MockResponse json(String body) {
        return new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .setBody(body);
    }
}
