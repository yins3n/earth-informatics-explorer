package com.earthinformatics.explorer.client;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.UpstreamRetry;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

/**
 * OpenSky Network - live aircraft state vectors.
 *
 * <p>Upstream: {@code https://opensky-network.org/api/states/all}
 * <p>Documented at https://opensky-network.org/apidoc/
 *
 * <p>Anonymous access is free but limited to a small number of requests per time window and to
 * the full-world endpoint. Supplying OAuth2 client credentials
 * ({@code EXPLORER_OPEN_SKY_CLIENT_ID} / {@code ..._CLIENT_SECRET}) raises both the rate limit and
 * the credit allowance, which matters because this endpoint holds a deliberately short cache TTL.
 *
 * <p>The access token is cached in memory and refreshed a minute before expiry. Because several
 * concurrent requests can miss the token at once, refresh is memoised: exactly one refresh runs
 * and the rest await its result (the same single-flight principle used for upstream documents).
 */
@Component
@Slf4j
public class OpenSkyClient {

    /** Refresh this many seconds before the token actually expires. */
    private static final long EXPIRY_SAFETY_MARGIN_SECONDS = 60;

    private final WebClient webClient;
    private final ExplorerProperties properties;
    private final AtomicReference<AccessToken> token = new AtomicReference<>();
    private final Mono<AccessToken> refreshLock;

    private record AccessToken(String value, Instant expiresAt) {

        boolean usable(Instant now) {
            return value != null && now.isBefore(expiresAt);
        }
    }

    public OpenSkyClient(WebClient webClient, ExplorerProperties properties) {
        this.webClient = webClient;
        this.properties = properties;
        // cache() makes the token fetch single-flight and replayable: concurrent callers share
        // one HTTP round trip to the identity provider.
        this.refreshLock = Mono.defer(this::requestToken).cache();
    }

    /**
     * Fetches aircraft state vectors, optionally restricted to a bounding box.
     *
     * @param west  Minimum longitude, or {@code null} for the whole world.
     * @param south Minimum latitude.
     * @param east  Maximum longitude.
     * @param north Maximum latitude.
     */
    public Mono<JsonNode> fetchStates(Double west, Double south, Double east, Double north) {
        ExplorerProperties.Upstreams.OpenSky config = properties.upstreams().openSky();
        if (!config.enabled()) {
            return Mono.empty();
        }

        MultiValueMap<String, String> query = new LinkedMultiValueMap<>();
        if (west != null && south != null && east != null && north != null) {
            query.add("lamin", String.valueOf(south));
            query.add("lomin", String.valueOf(west));
            query.add("lamax", String.valueOf(north));
            query.add("lomax", String.valueOf(east));
        }
        String uri = UriComponentsBuilder.fromUriString(config.baseUrl() + "/states/all")
                .queryParams(query)
                .build()
                .toUriString();

        Mono<String> bearerToken = configured()
                ? tokenMono().map(AccessToken::value)
                : Mono.just("");

        return bearerToken
                .flatMap(bearer -> {
                    WebClient.RequestHeadersSpec<?> request = webClient.get().uri(uri);
                    if (!bearer.isBlank()) {
                        request = request.header("Authorization", "Bearer " + bearer);
                    }
                    return request.retrieve().bodyToMono(JsonNode.class);
                })
                .timeout(properties.resilience().responseTimeout())
                .retryWhen(UpstreamRetry.backoff(
                                properties.resilience().retryBackoff(), properties.resilience().maxRetries())
                        // A 401 means the token died early; drop it so the retry gets a fresh one.
                        .doBeforeRetry(signal -> token.set(null)))
                .doOnError(error -> log.warn("OpenSky states request failed: {}", error.toString()));
    }

    public String statesUrl() {
        return properties.upstreams().openSky().baseUrl() + "/states/all";
    }

    private boolean configured() {
        ExplorerProperties.Upstreams.OpenSky config = properties.upstreams().openSky();
        return config.clientId() != null && !config.clientId().isBlank()
                && config.clientSecret() != null && !config.clientSecret().isBlank();
    }

    private Mono<AccessToken> tokenMono() {
        AccessToken current = token.get();
        if (current != null && current.usable(Instant.now().plusSeconds(EXPIRY_SAFETY_MARGIN_SECONDS))) {
            return Mono.just(current);
        }
        // One refresh at a time; every other caller receives the same result.
        return refreshLock.doOnNext(token::set);
    }

    private Mono<AccessToken> requestToken() {
        ExplorerProperties.Upstreams.OpenSky config = properties.upstreams().openSky();
        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "client_credentials");
        form.add("client_id", config.clientId());
        form.add("client_secret", config.clientSecret());
        if (config.audience() != null && !config.audience().isBlank()) {
            form.add("audience", config.audience());
        }

        return webClient.post()
                .uri(config.tokenUrl())
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .bodyValue(form)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(body -> new AccessToken(body.path("access_token").asText(null),
                        Instant.now().plusSeconds(
                                Math.max(60, body.path("expires_in").asLong(3600)))
                                .truncatedTo(ChronoUnit.SECONDS)))
                .doOnNext(fetched -> log.info("Obtained OpenSky OAuth2 token, expires {}",
                        fetched.expiresAt()))
                .doOnError(error -> log.warn("OpenSky token request failed, falling back to "
                        + "anonymous access: {}", error.toString()))
                // Anonymous access still works, just with tighter limits.
                .onErrorReturn(new AccessToken(null, Instant.now().plusSeconds(300)));
    }

    /** Diagnostics: true when the deployment has elevated (authenticated) access. */
    public boolean authenticated() {
        return configured();
    }

    /** Field order of the OpenSky {@code states} arrays, per the API documentation. */
    public static final Map<String, Integer> STATE_VECTOR_FIELDS = Map.ofEntries(
            Map.entry("icao24", 0),
            Map.entry("callsign", 1),
            Map.entry("originCountry", 2),
            Map.entry("timePosition", 3),
            Map.entry("lastContact", 4),
            Map.entry("longitude", 5),
            Map.entry("latitude", 6),
            Map.entry("baroAltitude", 7),
            Map.entry("onGround", 8),
            Map.entry("velocity", 9),
            Map.entry("trueTrack", 10),
            Map.entry("verticalRate", 11),
            Map.entry("geoAltitude", 13),
            Map.entry("squawk", 14),
            Map.entry("spi", 15),
            Map.entry("positionSource", 16));
}
