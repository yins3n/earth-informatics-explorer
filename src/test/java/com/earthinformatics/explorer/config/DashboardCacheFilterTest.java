package com.earthinformatics.explorer.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

class DashboardCacheFilterTest {

    private static String cacheControlFor(String method, String path) {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.method(HttpMethod.valueOf(method), path));
        exchange.getResponse().getHeaders().setCacheControl("unset");
        new DashboardCacheFilter().filter(exchange, ignored -> Mono.empty()).block();
        return exchange.getResponse().getHeaders().getFirst("Cache-Control");
    }

    @Test
    @DisplayName("the dashboard's own files must be revalidated, never served blind from cache")
    void dashboardFilesRevalidate() {
        assertThat(cacheControlFor("GET", "/")).isEqualTo("no-cache");
        assertThat(cacheControlFor("GET", "/index.html")).isEqualTo("no-cache");
        assertThat(cacheControlFor("GET", "/js/app.js")).isEqualTo("no-cache");
        assertThat(cacheControlFor("GET", "/css/app.css")).isEqualTo("no-cache");
    }

    @Test
    @DisplayName("vendored Cesium is version-pinned per build, so it may stay cached")
    void vendoredCesiumIsImmutable() {
        assertThat(cacheControlFor("GET", "/cesium/Cesium.js"))
                .isEqualTo("public, max-age=31536000, immutable");
        assertThat(cacheControlFor("GET", "/cesium/Workers/chunk-2ED5WI77.js"))
                .isEqualTo("public, max-age=31536000, immutable");
    }

    @Test
    @DisplayName("API responses and non-GET requests are left untouched")
    void leavesEverythingElseAlone() {
        assertThat(cacheControlFor("GET", "/api/v1/system/layers")).isEqualTo("unset");
        assertThat(cacheControlFor("GET", "/ws/telemetry")).isEqualTo("unset");
        assertThat(cacheControlFor("POST", "/js/app.js")).isEqualTo("unset");
    }
}
