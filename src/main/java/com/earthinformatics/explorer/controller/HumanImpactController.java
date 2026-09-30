package com.earthinformatics.explorer.controller;

import com.earthinformatics.explorer.client.AisStreamClient;
import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.query.HumanImpactQuery;
import com.earthinformatics.explorer.service.HumanImpactService;
import com.earthinformatics.explorer.util.QueryParameters;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Human impact: live commercial aviation and maritime traffic.
 *
 * <pre>
 * GET /api/v1/human-impact/flights?bbox=-10,35,5,60&amp;airborneOnly=true
 * GET /api/v1/human-impact/vessels
 * GET /api/v1/human-impact/summary
 * GET /api/v1/human-impact/ais/status
 * </pre>
 *
 * <p>Aircraft come from a polled REST resource with a 45 second cache; vessels are pushed over a
 * WebSocket and read from memory, so their endpoint degrades to an empty collection - flagged in
 * {@code meta.degraded} - whenever the AISStream connection is not {@code CONNECTED}.
 */
@RestController
@RequestMapping("/api/v1/human-impact")
public class HumanImpactController {

    private static final int MIN_RESULTS = 1;
    private static final int MAX_RESULTS = 8_000;

    private final HumanImpactService humanImpactService;
    private final AisStreamClient aisStreamClient;

    public HumanImpactController(HumanImpactService humanImpactService,
            AisStreamClient aisStreamClient) {
        this.humanImpactService = humanImpactService;
        this.aisStreamClient = aisStreamClient;
    }

    @GetMapping("/flights")
    public Mono<GeoJsonPayload> flights(
            @RequestParam(required = false) String bbox,
            @RequestParam(defaultValue = "8000") int maxResults,
            @RequestParam(defaultValue = "true") boolean airborneOnly) {
        return humanImpactService.flights(new HumanImpactQuery(
                QueryParameters.bbox(bbox),
                QueryParameters.intInRange("maxResults", maxResults, 8_000, MIN_RESULTS,
                        MAX_RESULTS),
                airborneOnly));
    }

    @GetMapping("/vessels")
    public Mono<GeoJsonPayload> vessels(
            @RequestParam(required = false) String bbox,
            @RequestParam(defaultValue = "5000") int maxResults) {
        return humanImpactService.vessels(new HumanImpactQuery(
                QueryParameters.bbox(bbox),
                QueryParameters.intInRange("maxResults", maxResults, 5_000, MIN_RESULTS,
                        MAX_RESULTS),
                false));
    }

    @GetMapping
    public Mono<GeoJsonPayload> all(
            @RequestParam(required = false) String bbox,
            @RequestParam(defaultValue = "8000") int maxResults,
            @RequestParam(defaultValue = "true") boolean airborneOnly) {
        HumanImpactQuery query = new HumanImpactQuery(
                QueryParameters.bbox(bbox),
                QueryParameters.intInRange("maxResults", maxResults, 8_000, MIN_RESULTS,
                        MAX_RESULTS),
                airborneOnly);
        return humanImpactService.flights(query)
                .zipWith(humanImpactService.vessels(query))
                .map(tuple -> Payloads.merge("human-impact", tuple.getT1(), tuple.getT2()));
    }

    @GetMapping("/summary")
    public Mono<DomainSummary> summary() {
        return humanImpactService.summary();
    }

    /**
     * Maritime connection health.
     *
     * <p>Exposed separately because it is the only way to tell "there are simply no vessels near
     * the camera" apart from "the AIS feed is broken" - two states that look identical on the
     * globe and mean very different things to the user.
     */
    @GetMapping("/ais/status")
    public Mono<Map<String, Object>> aisStatus() {
        return Mono.just(Map.of(
                "state", aisStreamClient.state().name(),
                "connected", aisStreamClient.state() == AisStreamClient.ConnectionState.CONNECTED,
                "messagesReceived", aisStreamClient.messagesReceived(),
                "reconnectAttempts", aisStreamClient.reconnectAttempts(),
                "stalenessSeconds", aisStreamClient.staleness().toSeconds(),
                "lastMessageAt", String.valueOf(aisStreamClient.lastMessageAt()),
                "states", java.util.List.of(
                        AisStreamClient.ConnectionState.values())));
    }
}
