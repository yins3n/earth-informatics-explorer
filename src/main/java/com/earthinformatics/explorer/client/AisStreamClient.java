package com.earthinformatics.explorer.client;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Schedulers;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

/**
 * AISStream.io - live maritime Automatic Identification System positions.
 *
 * <p>Unlike every other provider here, AIS is a <b>push</b> feed: the upstream opens a WebSocket
 * and streams position reports continuously, so there is nothing to poll. The service therefore
 * owns a long-lived connection and keeps the most recent fix per MMSI in memory; the REST layer
 * simply serves a short-TTL snapshot of that map.
 *
 * <p>Lifecycle and resilience:
 * <ul>
 *   <li>The connection is established on context refresh and disposed on shutdown.</li>
 *   <li>Dropped connections reconnect with capped exponential backoff and jitter, so a provider
 *       outage does not turn into a reconnect storm.</li>
 *   <li>The vessel map is bounded by {@code ais.maxTrackedVessels} and aged out by
 *       {@code ais.vesselTtl}, which is what stops a 24/7 feed from becoming a memory leak.</li>
 *   <li>Every state change is published on {@link #statusStream()} so the WebSocket broadcaster
 *       can tell the globe, honestly, when the maritime layer has no live source.</li>
 * </ul>
 *
 * <p>Subscription payload follows the AISStream v0 protocol: a JSON control frame sent on
 * connect, filtered by bounding box so the globe only receives what it can render.
 */
@Component
@Slf4j
public class AisStreamClient {

    /** Live connection state, surfaced on the telemetry channel and the health endpoint. */
    public enum ConnectionState {
        DISABLED,
        CONNECTING,
        CONNECTED,
        DEGRADED,
        STOPPED
    }

    /** Latest known position for one vessel. */
    public record VesselPosition(
            long mmsi,
            String name,
            String callSign,
            String shipType,
            double latitude,
            double longitude,
            double speedKnots,
            double courseDegrees,
            double headingDegrees,
            int navigationalStatus,
            Instant timestamp) {

        public GeoBox box() {
            return new GeoBox(latitude, longitude);
        }
    }

    /** Small carrier so callers do not depend on this class just to hold a coordinate. */
    public record GeoBox(double latitude, double longitude) {
    }

    private final ObjectMapper objectMapper;
    private final ExplorerProperties properties;
    private final ReactorNettyWebSocketClient webSocketClient;
    private final Map<Long, VesselPosition> vessels = new ConcurrentHashMap<>();
    private final AtomicReference<ConnectionState> state =
            new AtomicReference<>(ConnectionState.STOPPED);
    private final AtomicLong messagesReceived = new AtomicLong();
    private final AtomicLong reconnectAttempts = new AtomicLong();
    private final AtomicReference<Instant> lastMessageAt = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final Sinks.Many<VesselPosition> vesselSink =
            Sinks.many().multicast().onBackpressureBuffer(512, false);
    private final Sinks.Many<ConnectionState> statusSink =
            Sinks.many().multicast().onBackpressureBuffer(16, false);

    private volatile Disposable connection;

    public AisStreamClient(ObjectMapper objectMapper, ExplorerProperties properties) {
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.webSocketClient = new ReactorNettyWebSocketClient();
    }

    // ------------------------------------------------------------- lifecycle

    @PostConstruct
    public void start() {
        ExplorerProperties.Upstreams.Ais config = properties.upstreams().ais();
        if (!config.enabled()) {
            publishStatus(ConnectionState.DISABLED);
            log.info("AISStream disabled by configuration; maritime layer will report degraded");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        // Do not establish a socket from the context-refresh thread.
        Schedulers.boundedElastic().schedule(this::connect);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        Disposable current = connection;
        if (current != null && !current.isDisposed()) {
            current.dispose();
        }
        publishStatus(ConnectionState.STOPPED);
    }

    /**
     * Handshake headers.
     *
     * <p>Two things matter here. ReactorNetty's WebSocket client dereferences the header map
     * unconditionally, so {@code null} throws an NPE inside the handshake rather than producing a
     * connection attempt. And AISStream's v3 API authenticates the *upgrade request* with an
     * {@code Ocp-Apim-Subscription-Key} header, answering 401/404 without it - the subscription
     * frame is only honoured for sockets that were admitted with a valid key.
     */
    private HttpHeaders handshakeHeaders(ExplorerProperties.Upstreams.Ais config) {
        HttpHeaders headers = new HttpHeaders();
        String apiKey = config.apiKey();
        if (apiKey != null && !apiKey.isBlank()) {
            headers.set("Ocp-Apim-Subscription-Key", apiKey.strip());
        }
        return headers;
    }

    private void connect() {
        ExplorerProperties.Upstreams.Ais config = properties.upstreams().ais();
        publishStatus(ConnectionState.CONNECTING);

        connection = webSocketClient
                .execute(URI.create(config.url()), handshakeHeaders(config), session -> {
                    publishStatus(ConnectionState.CONNECTED);
                    log.info("AISStream connected to {}", config.url());

                    // textMessage builds the frame eagerly; send() streams it onto the wire.
                    Mono<Void> outbound = session.send(
                            Flux.just(session.textMessage(subscriptionPayload(config))));

                    Mono<Void> inbound = session.receive()
                            .map(WebSocketMessage::getPayloadAsText)
                            .doOnNext(this::handleMessage)
                            .then()
                            // Never let a subscriber exception escape into the connect loop.
                            .onErrorResume(error -> {
                                log.warn("AISStream inbound stream failed: {}", error.toString());
                                return Mono.empty();
                            });

                    return outbound.then(inbound);
                })
                .doOnError(error -> publishStatus(ConnectionState.DEGRADED))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, config.reconnectDelay())
                        .maxBackoff(config.reconnectMaxDelay())
                        .jitter(0.4d)
                        .doBeforeRetry(signal -> {
                            reconnectAttempts.incrementAndGet();
                            publishStatus(ConnectionState.DEGRADED);
                        }))
                .doFinally(signal -> {
                    if (running.get()) {
                        publishStatus(ConnectionState.DEGRADED);
                    }
                })
                .onErrorComplete()
                .subscribe();
    }

    /**
     * AISStream control frame. Bounding boxes are ordered {@code [minLat, minLon, maxLat, maxLon]}.
     */
    private String subscriptionPayload(ExplorerProperties.Upstreams.Ais config) {
        return """
                {
                  "UserAccountCode": "earth-informatics-explorer",
                  "APIKey": "%s",
                  "BBoxes": [[-90, -180, 90, 180]],
                  "Filters": {"Types": ["VesselPositionReport"]}
                }
                """.formatted(config.apiKey() == null ? "" : config.apiKey());
    }

    // --------------------------------------------------------------- parsing

    /** Parses one AISStream text frame; malformed frames are logged and skipped, never fatal. */
    void handleMessage(String payload) {
        if (payload == null || payload.isBlank()) {
            return;
        }
        JsonNode root;
        try {
            root = objectMapper.readTree(payload);
        } catch (JsonProcessingException malformed) {
            log.debug("Discarding malformed AISStream frame: {}", malformed.getOriginalMessage());
            return;
        }
        if (root == null || !"PositionReport".equals(root.path("MessageType").asText())) {
            return;
        }
        JsonNode report = root.path("Message").path("PositionReport");
        JsonNode meta = root.path("Message").path("MetaData");
        if (!report.isObject()) {
            return;
        }
        double latitude = report.path("Latitude").asDouble(Double.NaN);
        double longitude = report.path("Longitude").asDouble(Double.NaN);
        if (!com.earthinformatics.explorer.util.Geo.isValidPosition(longitude, latitude)) {
            return;
        }
        long mmsi = meta.path("MMSI").asLong(0);
        if (mmsi == 0) {
            return;
        }
        VesselPosition vessel = new VesselPosition(
                mmsi,
                clean(meta.path("ShipName").asText("")),
                clean(meta.path("CallSign").asText("")),
                clean(meta.path("ShipType").asText("")),
                latitude,
                longitude,
                report.path("SOG").asDouble(Double.NaN),
                report.path("COG").asDouble(Double.NaN),
                report.path("trueHeading").asDouble(Double.NaN),
                meta.path("NavigationalStatus").asInt(-1),
                Instant.ofEpochSecond(report.path("timeStamp").asLong(
                        Instant.now().getEpochSecond())));

        vessels.put(mmsi, vessel);
        messagesReceived.incrementAndGet();
        lastMessageAt.set(Instant.now());
        vesselSink.emitNext(vessel, Sinks.EmitFailureHandler.FAIL_FAST);

        if (vessels.size() % 500 == 0) {
            evictStale();
        }
    }

    /** Drops aged-out vessels, then trims the oldest entries if still above the cap. */
    void evictStale() {
        int maxTracked = properties.upstreams().ais().maxTrackedVessels();
        Instant cutoff = Instant.now().minus(properties.upstreams().ais().vesselTtl());
        vessels.values().removeIf(vessel -> vessel.timestamp().isBefore(cutoff));

        if (vessels.size() > maxTracked) {
            List<Map.Entry<Long, VesselPosition>> entries = new ArrayList<>(vessels.entrySet());
            entries.sort(Comparator.comparingLong(entry -> entry.getValue().timestamp().toEpochMilli()));
            int excess = vessels.size() - maxTracked;
            for (int i = 0; i < excess && i < entries.size(); i++) {
                vessels.remove(entries.get(i).getKey());
            }
            log.debug("Trimmed {} aged vessels from the AIS cache", excess);
        }
    }

    private static String clean(String value) {
        if (value == null) {
            return "";
        }
        String trimmed = value.strip();
        return trimmed.isEmpty() || "@@@".equals(trimmed) ? "" : trimmed;
    }

    // ------------------------------------------------------------- accessors

    /** Immutable snapshot of the live vessel map, newest first. */
    public List<VesselPosition> snapshot() {
        evictStale();
        List<VesselPosition> all = new ArrayList<>(vessels.values());
        all.sort(Comparator.comparing(VesselPosition::timestamp).reversed());
        return all;
    }

    public Flux<VesselPosition> vesselStream() {
        return vesselSink.asFlux();
    }

    public Flux<ConnectionState> statusStream() {
        return statusSink.asFlux();
    }

    public ConnectionState state() {
        return state.get();
    }

    public long messagesReceived() {
        return messagesReceived.get();
    }

    public long reconnectAttempts() {
        return reconnectAttempts.get();
    }

    public Instant lastMessageAt() {
        return lastMessageAt.get();
    }

    /** Age of the newest fix, or {@link Duration#ZERO} if nothing has arrived yet. */
    public Duration staleness() {
        Instant last = lastMessageAt.get();
        return last == null ? Duration.ZERO : Duration.between(last, Instant.now());
    }

    private void publishStatus(ConnectionState newState) {
        ConnectionState previous = state.getAndSet(newState);
        if (previous != newState) {
            log.info("AISStream state {} -> {}", previous, newState);
            statusSink.emitNext(newState, Sinks.EmitFailureHandler.FAIL_FAST);
        }
    }
}
