package com.earthinformatics.explorer.service;

import com.earthinformatics.explorer.client.AisStreamClient;
import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.TelemetryTick;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Builds the frame that is broadcast on {@code /ws/telemetry}.
 *
 * <p>The tick loop runs every few seconds, so this class must stay O(number of summaries), not
 * O(number of features). Each {@code summary()} it consumes is itself cached on the short tier,
 * which means the loop performs five map lookups per tick and zero upstream calls unless a TTL
 * has genuinely expired.
 *
 * <p>The {@code latest} list is the only place raw features appear, and it is hard-capped: a
 * dashboard that renders a pulse should not be able to make the server serialise a megabyte of
 * wildfire GeoJSON every five seconds.
 */
@Service
@Slf4j
public class TelemetryService {

    /** Maximum number of "something happened" items carried in a single tick. */
    static final int MAX_LATEST_EVENTS = 60;

    private final TectonicsService tectonicsService;
    private final AtmosphericsService atmosphericsService;
    private final OceansService oceansService;
    private final BiosphereService biosphereService;
    private final HumanImpactService humanImpactService;
    private final AisStreamClient aisStreamClient;
    private final ExplorerProperties properties;

    private final AtomicLong sequence = new AtomicLong();
    private final Instant startedAt = Instant.now();

    public TelemetryService(TectonicsService tectonicsService,
            AtmosphericsService atmosphericsService,
            OceansService oceansService,
            BiosphereService biosphereService,
            HumanImpactService humanImpactService,
            AisStreamClient aisStreamClient,
            ExplorerProperties properties) {
        this.tectonicsService = tectonicsService;
        this.atmosphericsService = atmosphericsService;
        this.oceansService = oceansService;
        this.biosphereService = biosphereService;
        this.humanImpactService = humanImpactService;
        this.aisStreamClient = aisStreamClient;
        this.properties = properties;
    }

    /**
     * Assembles one telemetry frame.
     *
     * @param clients Number of WebSocket subscribers, published so the HUD can show fan-in.
     */
    public Mono<TelemetryTick> buildTick(int clients) {
        Mono<DomainSummary> tectonics = tectonicsService.summary().onErrorReturn(
                DomainSummary.empty("tectonics"));
        Mono<DomainSummary> atmospherics = atmosphericsService.summary().onErrorReturn(
                DomainSummary.empty("atmospherics"));
        Mono<DomainSummary> oceans = oceansService.summary().onErrorReturn(
                DomainSummary.empty("oceans"));
        Mono<DomainSummary> biosphere = biosphereService.summary().onErrorReturn(
                DomainSummary.empty("biosphere"));
        Mono<DomainSummary> humanImpact = humanImpactService.summary().onErrorReturn(
                DomainSummary.empty("human-impact"));

        return Mono.zip(tectonics, atmospherics, oceans, biosphere, humanImpact)
                .map(tuple -> {
                    Map<String, DomainSummary> summaries = new LinkedHashMap<>();
                    summaries.put("tectonics", tuple.getT1());
                    summaries.put("atmospherics", tuple.getT2());
                    summaries.put("oceans", tuple.getT3());
                    summaries.put("biosphere", tuple.getT4());
                    summaries.put("human-impact", tuple.getT5());

                    return new TelemetryTick(
                            sequence.incrementAndGet(),
                            Instant.now().toEpochMilli(),
                            properties.websocket().tickInterval().toMillis(),
                            clients,
                            Instant.now(),
                            summaries,
                            latestEvents(tuple.getT1(), tuple.getT4()));
                });
    }

    /**
     * Flattens the newest events from the tectonic and biosphere summaries into a compact,
     * fixed-size list. The client draws these as the "live pulse" ring, so the coordinates and
     * a headline number are all it needs.
     */
    private List<TelemetryTick.TelemetryEvent> latestEvents(DomainSummary tectonics,
            DomainSummary biosphere) {
        List<TelemetryTick.TelemetryEvent> events = new ArrayList<>(MAX_LATEST_EVENTS);

        for (var feature : tectonics.latest()) {
            double[] position = position(feature);
            if (position == null) {
                continue;
            }
            double magnitude = property(feature, "magnitude");
            events.add(new TelemetryTick.TelemetryEvent(
                    "tectonics",
                    "earthquake",
                    text(feature, "id"),
                    position[1],
                    position[0],
                    magnitude,
                    text(feature, "place"),
                    (long) property(feature, "eventTime")));
        }

        for (var feature : biosphere.latest()) {
            double[] position = position(feature);
            if (position == null) {
                continue;
            }
            double frp = property(feature, "frp");
            events.add(new TelemetryTick.TelemetryEvent(
                    "biosphere",
                    "wildfire",
                    text(feature, "id"),
                    position[1],
                    position[0],
                    frp,
                    text(feature, "satellite"),
                    (long) property(feature, "acquiredAt")));
        }

        return events.size() <= MAX_LATEST_EVENTS
                ? List.copyOf(events)
                : List.copyOf(events.subList(0, MAX_LATEST_EVENTS));
    }

    /** Live connection health for the maritime layer, surfaced on the tick. */
    public Map<String, Object> aisStatus() {
        return Map.of(
                "state", aisStreamClient.state().name(),
                "messages", aisStreamClient.messagesReceived(),
                "reconnects", aisStreamClient.reconnectAttempts(),
                "stalenessSeconds", aisStreamClient.staleness().toSeconds(),
                "enabled", properties.upstreams().ais().enabled());
    }

    public long uptimeSeconds() {
        return java.time.Duration.between(startedAt, Instant.now()).toSeconds();
    }

    private static double[] position(com.fasterxml.jackson.databind.JsonNode feature) {
        com.fasterxml.jackson.databind.JsonNode geometry = feature.path("geometry");
        if (!"Point".equals(geometry.path("type").asText())) {
            return null;
        }
        var coordinates = geometry.path("coordinates");
        return coordinates.isArray() && coordinates.size() >= 2
                ? new double[]{coordinates.get(0).asDouble(), coordinates.get(1).asDouble()}
                : null;
    }

    private static double property(com.fasterxml.jackson.databind.JsonNode feature, String name) {
        var value = feature.path("properties").path(name);
        if (value.isNumber()) {
            return value.asDouble();
        }
        if (value.isTextual()) {
            try {
                return Double.parseDouble(value.asText().strip());
            } catch (NumberFormatException notANumber) {
                return Double.NaN;
            }
        }
        return Double.NaN;
    }

    private static String text(com.fasterxml.jackson.databind.JsonNode feature, String name) {
        var value = feature.path("properties").path(name);
        return value.isMissingNode() || value.isNull() ? "" : value.asText();
    }
}
