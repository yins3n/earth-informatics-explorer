package com.earthinformatics.explorer.client;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.UpstreamRetry;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * USGS Earthquake Hazards Program - real-time GeoJSON feed.
 *
 * <p>Upstream: {@code https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/<feed>.geojson}
 * <p>Documented at https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
 *
 * <p>No API key, no authentication, and the payloads are GeoJSON already, so this client is a
 * pure pass-through. USGS asks for a descriptive user agent, which {@code WebClientConfig}
 * supplies globally.
 */
@Component
@Slf4j
public class UsgsClient {

    /** Feed identifiers whose cadence matches our cache TTL. */
    public static final String ALL_HOUR = "all_hour";
    public static final String ALL_DAY = "all_day";
    public static final String ALL_WEEK = "all_week";
    public static final String ALL_MONTH = "all_month";

    private final WebClient webClient;
    private final ExplorerProperties properties;

    public UsgsClient(WebClient webClient, ExplorerProperties properties) {
        this.webClient = webClient;
        this.properties = properties;
    }

    /**
     * Fetches one summary feed.
     *
     * @return the raw {@code FeatureCollection} document exactly as published, or
     *         {@code Mono.empty()} if the feed name is unknown to the service.
     */
    public Mono<JsonNode> fetchFeed(String feed) {
        ExplorerProperties.Upstreams.Usgs config = properties.upstreams().usgs();
        if (!config.feeds().contains(feed)) {
            log.warn("Refusing unknown USGS feed '{}'; allowed feeds are {}", feed, config.feeds());
            return Mono.empty();
        }
        String uri = "%s/summary/%s.geojson".formatted(config.baseUrl(), feed);
        return webClient.get()
                .uri(uri)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(UpstreamRetry.backoff(
                        properties.resilience().retryBackoff(),
                        properties.resilience().maxRetries()))
                .timeout(properties.resilience().responseTimeout())
                .doOnNext(node -> log.debug("USGS feed {} returned {} features",
                        feed, node.path("features").size()))
                .doOnError(error -> log.warn("USGS feed {} failed: {}", feed, error.toString()));
    }

    /** Endpoint URL for documentation/provenance blocks. */
    public String feedUrl(String feed) {
        return "%s/summary/%s.geojson".formatted(properties.upstreams().usgs().baseUrl(), feed);
    }

    public ExplorerProperties.Upstreams.Usgs config() {
        return properties.upstreams().usgs();
    }
}
