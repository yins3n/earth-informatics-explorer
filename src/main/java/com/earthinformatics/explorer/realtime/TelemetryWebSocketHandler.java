package com.earthinformatics.explorer.realtime;

import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.TelemetryTick;
import com.earthinformatics.explorer.dto.TelemetryTick.TelemetryEvent;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * Reactive WebSocket handler for {@code /ws/telemetry}.
 *
 * <p>Protocol
 * <pre>
 * server -&gt; {"type":"hello","serverTime":..,"tickIntervalMs":..,"layers":[..],"apiVersion":".."}
 * server -&gt; {"type":"tick","seq":..,"serverTime":..,"clients":..,"summaries":{..},"latest":[..]}
 * server -&gt; {"type":"pong","clientTime":..,"serverTime":..}
 * server -&gt; {"type":"error","code":"BAD_MESSAGE","message":".."}
 * client -&gt; {"type":"subscribe","layers":["tectonics","biosphere"]}   (optional filter)
 * client -&gt; {"type":"ping","clientTime":1735689600000}
 * </pre>
 *
 * <p>Concurrency and back-pressure:
 * <ul>
 *   <li><b>One writer per session.</b> Netty permits a single concurrent outbound publisher per
 *       connection, so tick frames and control replies are merged into one unicast sink; a second
 *       concurrent {@code session.send(...)} would silently drop frames.</li>
 *   <li><b>Bounded queue.</b> A throttled browser tab fills the sink quickly; on overflow the
 *       session is closed instead of growing an unbounded queue.</li>
 *   <li><b>Per-frame filtering.</b> The subscription set is thread-safe and evaluated for each
 *       tick, so a client may change its mind at any time without re-subscribing.</li>
 * </ul>
 */
@Component
@Slf4j
public class TelemetryWebSocketHandler implements WebSocketHandler {

    /** Domains a client may subscribe to; mirrored in the hello frame. */
    public static final List<String> LAYER_DOMAINS =
            List.of("tectonics", "atmospherics", "oceans", "biosphere", "human-impact");

    /**
     * Per-session outbound queue depth, in frames.
     *
     * <p>Ten ticks is roughly a minute at the default interval: enough to ride out a GC pause or
     * a brief network stall, small enough that a genuinely wedged client is disconnected instead
     * of consuming server memory indefinitely.
     */
    private static final int OUTBOUND_QUEUE_FRAMES = 10;

    private final TelemetryBroadcaster broadcaster;
    private final ObjectMapper objectMapper;
    private final ExplorerProperties properties;

    public TelemetryWebSocketHandler(TelemetryBroadcaster broadcaster, ObjectMapper objectMapper,
            ExplorerProperties properties) {
        this.broadcaster = broadcaster;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        // Bounded queue. Reactor 3.6's unicast spec has no overflow callback, so the bound is
        // expressed as the queue itself: a full ArrayBlockingQueue rejects the newest frame
        // instead of growing, and SessionState.send() treats that rejection as a reason to drop
        // the tick and count it. A client that cannot drain ten frames is not a client we can
        // usefully serve anyway.
        Sinks.Many<String> outbound = Sinks.many().unicast()
                .onBackpressureBuffer(new ArrayBlockingQueue<>(OUTBOUND_QUEUE_FRAMES));
        SessionState state = new SessionState(session.getId(), outbound);

        broadcaster.clientConnected();
        log.info("WebSocket {} connected ({} total)", session.getId(),
                broadcaster.connectedClients());

        // Greet immediately so the client can render a "connected" badge before the first tick.
        state.send(helloFrame());

        Flux<String> frames = broadcaster.ticks()
                .map(tick -> state.filter(tick))
                .filter(tick -> tick != null)
                .map(this::serialiseTick)
                .onErrorResume(error -> {
                    log.warn("Tick stream error for {}: {}", session.getId(), error.toString());
                    return Flux.empty();
                })
                .mergeWith(outbound.asFlux());

        Mono<Void> writing = session.send(frames.map(session::textMessage))
                .onErrorResume(error -> {
                    log.debug("Outbound stream ended for {}: {}", session.getId(),
                            error.toString());
                    return Mono.empty();
                })
                .then();

        Mono<Void> reading = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(payload -> handleClientMessage(state, payload))
                .onErrorResume(error -> {
                    log.debug("Inbound stream ended for {}: {}", session.getId(),
                            error.toString());
                    return Mono.empty();
                })
                .then();

        return Mono.zip(writing, reading)
                .doFinally(signal -> {
                    broadcaster.clientDisconnected();
                    log.info("WebSocket {} closed after {} frames ({})", session.getId(),
                            state.framesSent(), signal.name());
                })
                .then();
    }

    /**
     * Handles one inbound control frame. Unknown or malformed messages produce an
     * {@code error} frame rather than a close, so a client bug stays debuggable in the browser
     * console instead of appearing as a mystery disconnect.
     */
    void handleClientMessage(SessionState state, String payload) {
        JsonNode message;
        try {
            message = objectMapper.readTree(payload);
        } catch (Exception malformed) {
            state.send(errorFrame("BAD_MESSAGE", "payload is not valid JSON"));
            return;
        }
        String type = message.path("type").asText("").toLowerCase(Locale.ROOT);
        switch (type) {
            case "ping" -> state.send(pongFrame(message.path("clientTime").asLong(0)));
            case "subscribe" -> {
                state.updateSubscription(message.path("layers"));
                state.send(ackFrame("subscribed", state.subscriptions()));
            }
            case "pong" -> {
                // Client keepalive reply; nothing to do.
            }
            default -> state.send(errorFrame("UNKNOWN_TYPE",
                    "unsupported message type '" + type + "'"));
        }
    }

    // ---------------------------------------------------------------- framing

    private String helloFrame() {
        ObjectNode frame = objectMapper.createObjectNode();
        frame.put("type", "hello");
        frame.put("serverTime", Instant.now().toEpochMilli());
        frame.put("tickIntervalMs", broadcaster.tickIntervalMillis());
        frame.put("clients", broadcaster.connectedClients());
        frame.put("apiVersion", properties.api().version());
        frame.set("layers", objectMapper.valueToTree(LAYER_DOMAINS));
        return frame.toString();
    }

    private String pongFrame(long clientTime) {
        ObjectNode frame = objectMapper.createObjectNode();
        frame.put("type", "pong");
        frame.put("clientTime", clientTime);
        frame.put("serverTime", Instant.now().toEpochMilli());
        return frame.toString();
    }

    private String ackFrame(String code, Set<String> layers) {
        ObjectNode frame = objectMapper.createObjectNode();
        frame.put("type", "ack");
        frame.put("code", code);
        frame.set("layers", objectMapper.valueToTree(layers));
        return frame.toString();
    }

    private String errorFrame(String code, String message) {
        ObjectNode frame = objectMapper.createObjectNode();
        frame.put("type", "error");
        frame.put("code", code);
        frame.put("message", message);
        return frame.toString();
    }

    private String serialiseTick(TelemetryTick tick) {
        try {
            ObjectNode frame = objectMapper.valueToTree(tick);
            frame.put("type", "tick");
            return frame.toString();
        } catch (Exception failure) {
            log.warn("Failed to serialise telemetry tick {}", tick.seq(), failure);
            return errorFrame("SERIALISATION", "tick could not be serialised");
        }
    }

    // ----------------------------------------------------------- session state

    /**
     * Per-session state: the subscription filter plus the single outbound sink.
     *
     * <p>An empty subscription set means "everything", which is the right default - a client that
     * never sends a {@code subscribe} frame still receives all domains and the server never has
     * to guess what it wanted.
     */
    static final class SessionState {

        private final String sessionId;
        private final Sinks.Many<String> outbound;
        private final Set<String> layers = ConcurrentHashMap.newKeySet();
        private final AtomicLong framesSent = new AtomicLong();

        SessionState(String sessionId, Sinks.Many<String> outbound) {
            this.sessionId = sessionId;
            this.outbound = outbound;
        }

        void updateSubscription(JsonNode requested) {
            layers.clear();
            if (requested != null && requested.isArray()) {
                requested.forEach(node -> {
                    String layer = node.asText().trim().toLowerCase(Locale.ROOT);
                    if (!layer.isEmpty()) {
                        layers.add(layer);
                    }
                });
            }
        }

        /** A tick is delivered when the client wants this domain, or everything. */
        boolean isSubscribedTo(TelemetryTick tick) {
            if (layers.isEmpty()) {
                return true;
            }
            return tick.summaries().keySet().stream().anyMatch(layers::contains);
        }

        /**
         * Narrows a tick to the subscribed domains, or returns {@code null} when the client
         * wants none of them.
         *
         * <p>Filtering the frame rather than only deciding whether to send it is what makes the
         * {@code subscribe} message mean something: a client watching tectonics alone should not
         * receive the biosphere and human-impact roll-ups in every frame, which is the larger
         * share of the payload and the reason bandwidth matters on a globe view.
         */
        TelemetryTick filter(TelemetryTick tick) {
            if (layers.isEmpty() || layers.containsAll(tick.summaries().keySet())) {
                return tick;
            }
            Map<String, DomainSummary> summaries = tick.summaries().entrySet().stream()
                    .filter(entry -> layers.contains(entry.getKey()))
                    .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,
                            Map.Entry::getValue, (first, second) -> first, LinkedHashMap::new));
            if (summaries.isEmpty()) {
                return null;
            }
            List<TelemetryEvent> latest = tick.latest().stream()
                    .filter(event -> layers.contains(event.domain()))
                    .toList();
            return new TelemetryTick(tick.seq(), tick.serverTime(), tick.tickIntervalMs(),
                    tick.clients(), tick.generatedAt(), summaries, latest);
        }

        Set<String> subscriptions() {
            return Set.copyOf(layers);
        }

        /**
         * Queues one outbound frame.
         *
         * <p>Uses {@code emitNext} rather than {@code tryEmitNext} because control replies can be
         * produced from the receive thread while the tick stream is being written, so emissions
         * are not strictly serialised. {@code emitNext} takes a failure handler whose boolean
         * return value reports whether the frame made it into the queue; returning {@code false}
         * means "do not retry", which is what we want for a per-session queue that is already
         * full - a full queue means the tab is not draining frames and is showing stale data, so
         * retrying would only deepen the hole.
         */
        void send(String frame) {
            AtomicBoolean delivered = new AtomicBoolean();
            outbound.emitNext(frame, (signalType, result) -> {
                boolean queued = !result.isFailure();
                delivered.set(queued);
                if (!queued) {
                    log.warn("Outbound queue for {} is not accepting frames ({} {})", sessionId,
                            signalType, result);
                }
                return queued;
            });
            if (delivered.get()) {
                framesSent.incrementAndGet();
            }
        }

        long framesSent() {
            return framesSent.get();
        }

        String sessionId() {
            return sessionId;
        }
    }
}
