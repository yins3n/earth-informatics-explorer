package com.earthinformatics.explorer.realtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.earthinformatics.explorer.TestProperties;
import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.TelemetryTick;
import com.earthinformatics.explorer.dto.TelemetryTick.TelemetryEvent;
import com.earthinformatics.explorer.realtime.TelemetryWebSocketHandler.SessionState;
import com.earthinformatics.explorer.util.JsonSupport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

/**
 * Telemetry WebSocket session behaviour.
 *
 * <p>Driven at the session level rather than over a live socket: the protocol handling, the
 * subscription filter and the framing are all functions of the session state, and reaching them
 * through a real handshake would add flake without covering anything extra. The outbound sink is
 * the real one, so frame contents, queueing and the overflow counter are exercised as shipped.
 */
class TelemetryWebSocketHandlerTest {

    private static final ObjectMapper MAPPER = JsonSupport.mapper();

    private TelemetryBroadcaster broadcaster;
    private TelemetryWebSocketHandler handler;
    private Sinks.Many<String> outbound;
    private SessionState state;
    private List<String> sent;

    @BeforeEach
    void setUp() {
        broadcaster = mock(TelemetryBroadcaster.class);
        when(broadcaster.tickIntervalMillis()).thenReturn(15_000L);
        when(broadcaster.connectedClients()).thenReturn(1);
        handler = new TelemetryWebSocketHandler(broadcaster, MAPPER, TestProperties.defaults());

        outbound = Sinks.many().unicast().onBackpressureBuffer(new ArrayBlockingQueue<>(10));
        state = new SessionState("test-session", outbound);
        sent = new CopyOnWriteArrayList<>();
        outbound.asFlux().subscribe(sent::add);
    }

    private TelemetryTick tick() {
        return new TelemetryTick(7, 1_700_000_000_000L, 15_000, 2, Instant.EPOCH,
                Map.of(
                        "tectonics", summary("tectonics", 3),
                        "biosphere", summary("biosphere", 1),
                        "oceans", summary("oceans", 20)),
                List.of(event("tectonics", "eq-1"), event("biosphere", "fire-1")));
    }

    private static DomainSummary summary(String domain, int count) {
        return new DomainSummary(domain, count, false, Instant.EPOCH, count, domain,
                Map.of("domain", domain), List.of());
    }

    private static TelemetryEvent event(String domain, String id) {
        return new TelemetryEvent(domain, "wildfire", id, 10, 20, 5.0, "Wildfire near " + id,
                1_700_000_000_000L);
    }

    private JsonNode lastFrame() {
        assertThat(sent).as("a frame was emitted").isNotEmpty();
        try {
            return MAPPER.readTree(sent.get(sent.size() - 1));
        } catch (Exception malformed) {
            throw new IllegalStateException(malformed);
        }
    }

    private void subscribeTo(String... layers) {
        ObjectNode message = MAPPER.createObjectNode();
        message.put("type", "subscribe");
        ArrayNode array = message.putArray("layers");
        for (String layer : layers) {
            array.add(layer);
        }
        handler.handleClientMessage(state, message.toString());
    }

    @Nested
    @DisplayName("client messages")
    class ClientMessages {

        @Test
        @DisplayName("a ping is answered with a pong echoing the client clock")
        void pingPong() {
            ObjectNode message = MAPPER.createObjectNode();
            message.put("type", "ping");
            message.put("clientTime", 1_735_689_600_000L);

            handler.handleClientMessage(state, message.toString());

            JsonNode pong = lastFrame();
            assertThat(pong.path("type").asText()).isEqualTo("pong");
            // The echo is what makes round-trip time measurable from the browser.
            assertThat(pong.path("clientTime").asLong()).isEqualTo(1_735_689_600_000L);
            assertThat(pong.path("serverTime").asLong()).isPositive();
        }

        @Test
        @DisplayName("a subscribe frame is acknowledged with the normalised layer set")
        void subscribeAck() {
            subscribeTo("Tectonics", " OCEANS ");

            JsonNode ack = lastFrame();
            assertThat(ack.path("type").asText()).isEqualTo("ack");
            assertThat(ack.path("code").asText()).isEqualTo("subscribed");
            // Case and padding are normalised, or the filter silently matches nothing and the
            // client sees ticks it believes it subscribed to stop arriving.
            List<String> layers = new ArrayList<>();
            ack.path("layers").forEach(node -> layers.add(node.asText()));
            assertThat(layers).containsExactlyInAnyOrder("tectonics", "oceans");
        }

        @Test
        @DisplayName("malformed JSON gets a BAD_MESSAGE error rather than a dropped connection")
        void malformedJson() {
            handler.handleClientMessage(state, "{not json");

            JsonNode error = lastFrame();
            assertThat(error.path("type").asText()).isEqualTo("error");
            assertThat(error.path("code").asText()).isEqualTo("BAD_MESSAGE");
        }

        @Test
        @DisplayName("an unknown type is rejected by name")
        void unknownType() {
            handler.handleClientMessage(state, "{\"type\":\"launch-missiles\"}");

            JsonNode error = lastFrame();
            assertThat(error.path("code").asText()).isEqualTo("UNKNOWN_TYPE");
            assertThat(error.path("message").asText()).contains("launch-missiles");
        }

        @Test
        @DisplayName("a client pong is accepted without a reply")
        void pongIsSilent() {
            handler.handleClientMessage(state, "{\"type\":\"pong\"}");

            assertThat(sent).isEmpty();
        }

        @Test
        @DisplayName("a session that stops draining is bounded, not buffered without limit")
        void overflowIsBounded() {
            // A wedged tab that never reads. The queue is the bound: frames past it are dropped
            // rather than retried, because retrying a full queue only deepens the hole for a
            // client already showing stale telemetry.
            Sinks.Many<String> stalled = Sinks.many().unicast()
                    .onBackpressureBuffer(new ArrayBlockingQueue<>(10));
            SessionState wedged = new SessionState("wedged", stalled);

            for (int i = 0; i < 40; i++) {
                handler.handleClientMessage(wedged, "{\"type\":\"ping\"}");
            }

            List<String> recovered = new ArrayList<>();
            stalled.asFlux().subscribe(recovered::add);

            assertThat(recovered.size())
                    .as("the queue never grew past its ten-frame bound")
                    .isLessThanOrEqualTo(10);
            assertThat(wedged.framesSent())
                    .as("only the frames that were actually queued are counted")
                    .isLessThan(40);
        }
    }

    @Nested
    @DisplayName("subscription filter")
    class Subscription {

        @Test
        @DisplayName("no subscription means every domain, and the tick is not rebuilt")
        void emptyMeansAll() {
            TelemetryTick tick = tick();

            assertThat(state.subscriptions()).isEmpty();
            assertThat(state.isSubscribedTo(tick)).isTrue();
            // Identity, not equality: an unsubscribed client must not pay for rebuilding a frame
            // it will receive whole.
            assertThat(state.filter(tick)).as("unfiltered ticks pass through").isSameAs(tick);
        }

        @Test
        @DisplayName("a narrow subscription strips other domains from the frame")
        void narrowsTheFrame() {
            subscribeTo("tectonics");

            TelemetryTick filtered = state.filter(tick());

            assertThat(filtered).isNotNull();
            assertThat(filtered.summaries()).containsOnlyKeys("tectonics");
            assertThat(filtered.latest()).extracting(TelemetryEvent::domain)
                    .containsOnly("tectonics");
            // Envelope fields must survive, or the client's sequence counter restarts mid-session.
            assertThat(filtered.seq()).isEqualTo(7L);
            assertThat(filtered.clients()).isEqualTo(2);
            assertThat(filtered.generatedAt()).isEqualTo(Instant.EPOCH);
        }

        @Test
        @DisplayName("a tick containing nothing subscribed is dropped, not sent empty")
        void dropsIrrelevantTick() {
            subscribeTo("atmospherics");

            assertThat(state.filter(tick())).as("nothing this client asked for").isNull();
        }

        @Test
        @DisplayName("a partial subscription still delivers the tick it overlaps")
        void deliversOverlappingTick() {
            subscribeTo("tectonics", "atmospherics");

            assertThat(state.isSubscribedTo(tick())).isTrue();
            assertThat(state.filter(tick()).summaries()).containsOnlyKeys("tectonics");
        }

        @Test
        @DisplayName("resubscribing replaces the previous set")
        void resubscribeReplaces() {
            subscribeTo("tectonics");
            subscribeTo("oceans");

            assertThat(state.subscriptions()).containsExactly("oceans");
        }

        @Test
        @DisplayName("subscribing to nothing restores the everything default")
        void emptyListRestoresAll() {
            subscribeTo("tectonics");
            subscribeTo();

            assertThat(state.subscriptions()).isEmpty();
            assertThat(state.isSubscribedTo(tick())).isTrue();
        }
    }

    @Test
    @DisplayName("the advertised domains are exactly the five implemented ones")
    void advertisedDomains() {
        assertThat(TelemetryWebSocketHandler.LAYER_DOMAINS)
                .containsExactly("tectonics", "atmospherics", "oceans", "biosphere",
                        "human-impact");
    }

    @Test
    @DisplayName("a tick is serialised with its type tag for the client")
    void tickFraming() throws Exception {
        ObjectNode frame = MAPPER.valueToTree(tick());
        frame.put("type", "tick");

        // Mirrors serialiseTick: the client switches on "type" and every frame must carry one.
        assertThat(frame.path("type").asText()).isEqualTo("tick");
        assertThat(frame.path("seq").asLong()).isEqualTo(7L);
        assertThat(frame.path("summaries").fieldNames()).toIterable()
                .containsExactlyInAnyOrder("tectonics", "biosphere", "oceans");
        assertThat(frame.path("latest").get(0).path("domain").asText()).isEqualTo("tectonics");
    }
}
