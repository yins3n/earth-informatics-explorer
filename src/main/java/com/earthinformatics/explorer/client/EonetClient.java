package com.earthinformatics.explorer.client;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.UnusableRepresentationException;
import com.earthinformatics.explorer.util.UpstreamRetry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.util.MultiValueMap;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

/**
 * NASA EONET (Event Ontology) v3 - open natural hazards, filtered to active volcanoes.
 *
 * <p>Upstream: {@code https://eonet.gsfc.nasa.gov/api/v3/events?status=open&category=volcanoes}
 * <p>Documented at https://eonet.gsfc.nasa.gov/docs/v3
 *
 * <p>EONET is one of the few planetary APIs that publishes an <em>alert level</em> derived from
 * the Volcanic Explosivity Index and the USGS colour scale, which we forward verbatim so the
 * renderer can colour markers by aviation risk rather than inventing its own scheme.
 */
@Component
@Slf4j
public class EonetClient {

    /**
     * Upper bound on the look-back window. EONET keeps decade-scale event history, but a
     * ten-year pull is never what a dashboard wants, and it is what an unbounded parameter
     * invites.
     */
    public static final int MAX_LOOKBACK_DAYS = 3_650;

    private final WebClient webClient;
    private final ExplorerProperties properties;
    private final ObjectMapper objectMapper;

    public EonetClient(WebClient webClient, ExplorerProperties properties,
            ObjectMapper objectMapper) {
        this.webClient = webClient;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    /**
     * @param category EONET category, e.g. {@code volcanoes}, {@code severeStorms}, {@code seaIce}.
     * @param days     Look-back window in days; {@code -1} means "all open events".
     * @param limit    Maximum number of events (EONET caps this at 1000).
     */
    public Mono<JsonNode> fetchEvents(String category, int days, int limit) {
        ExplorerProperties.Upstreams.Eonet config = properties.upstreams().eonet();
        int effectiveLimit = Math.max(1, Math.min(limit <= 0 ? config.maxLimit() : limit,
                config.maxLimit()));

        MultiValueMap<String, String> query = new LinkedMultiValueMap<>();
        query.add("status", "open");
        query.add("category", category);
        query.add("limit", Integer.toString(effectiveLimit));
        if (days > 0) {
            // defaultDays is the *sustained* window, not a ceiling: a client asking for a year
            // of volcanoes must not be silently truncated to a month.
            query.add("days", Integer.toString(Math.min(days, MAX_LOOKBACK_DAYS)));
        }

        String uri = UriComponentsBuilder.fromUriString(config.baseUrl() + "/events")
                .queryParams(query)
                .build()
                .toUriString();

        return webClient.get()
                .uri(uri)
                // EONET content-negotiates and answers *RSS* to a client that does not state a
                // preference, with HTTP 200. Jackson then rejects the body and the caller sees a
                // confusing UnsupportedMediaTypeException instead of "wrong content type".
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                // Read as String and parse by hand. EONET is fronted by a load balancer whose
                // nodes disagree about the default representation: the same URL, with the same
                // Accept header, intermittently answers application/rss+xml with HTTP 200, and
                // bodyToMono(JsonNode.class) then fails on a *200* with a media-type error.
                // Parsing the bytes ourselves is the only version of this call that is correct
                // on every node.
                .bodyToMono(String.class)
                .map(this::parseEvents)
                .retryWhen(UpstreamRetry.backoff(
                        properties.resilience().retryBackoff(), properties.resilience().maxRetries()))
                .timeout(properties.resilience().responseTimeout())
                .doOnNext(node -> log.debug("EONET category {} returned {} events",
                        category, node.path("events").size()))
                .doOnError(error -> log.warn("EONET category {} failed: {}", category,
                        error.toString()));
    }

    /**
     * Parses an EONET document, rejecting the RSS representation with an explicit message.
     *
     * <p>The failure is an {@link UnusableRepresentationException} rather than a plain
     * {@link IllegalStateException} on purpose: it is classified transient by
     * {@link UpstreamRetry#isTransient(Throwable)}, so the next request goes to a node that
     * answers JSON. Throwing a non-retryable type here turned a one-bad-response blip into a
     * degraded volcano layer for the whole cache window.
     */
    private JsonNode parseEvents(String body) {
        if (body == null || body.isBlank()) {
            throw new UnusableRepresentationException("EONET returned an empty body");
        }
        String head = body.stripLeading();
        if (head.startsWith("<")) {
            throw new UnusableRepresentationException(
                    "EONET answered with an XML/RSS representation instead of JSON");
        }
        try {
            return objectMapper.readTree(body);
        } catch (JsonProcessingException malformed) {
            throw new UnusableRepresentationException("EONET body was not valid JSON: "
                    + malformed.getOriginalMessage(), malformed);
        }
    }

    /** Categories this deployment exposes through the tectonics endpoint. */
    public static List<String> supportedCategories() {
        return List.of("volcanoes");
    }

    public String eventsUrl(String category) {
        return properties.upstreams().eonet().baseUrl() + "/events?status=open&category="
                + category;
    }
}
