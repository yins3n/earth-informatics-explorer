package com.earthinformatics.explorer.controller;

import com.earthinformatics.explorer.config.WebSocketConfig;
import com.earthinformatics.explorer.dto.TelemetryTick;
import com.earthinformatics.explorer.realtime.TelemetryBroadcaster;
import com.earthinformatics.explorer.realtime.TelemetryWebSocketHandler;
import com.earthinformatics.explorer.service.TelemetryService;
import java.util.List;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Telemetry: server-sent-events mirror of the WebSocket, plus broker health.
 *
 * <p>Why ship both transports? The globe uses the WebSocket. But
 * {@code curl -N localhost:8080/api/v1/telemetry/stream} is the fastest way to prove the tick loop
 * works without a browser, and {@code EventSource} needs no library. Both are fed by the same
 * broadcaster, so the two views can never disagree.
 */
@RestController
@RequestMapping("/api/v1/telemetry")
public class TelemetryController {

    private final TelemetryBroadcaster broadcaster;
    private final TelemetryService telemetryService;

    public TelemetryController(TelemetryBroadcaster broadcaster, TelemetryService telemetryService) {
        this.broadcaster = broadcaster;
        this.telemetryService = telemetryService;
    }

    /**
     * Server-sent events on the same frame cadence as the WebSocket.
     *
     * <p>The SSE {@code id} is the tick sequence number, which is precisely what a browser sends
     * back as {@code Last-Event-ID} when it reconnects. Because the broadcaster's sink is
     * multicast with a bounded buffer, a reconnecting client resumes from the newest frame
     * instead of replaying history it has already drawn.
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<TelemetryTick>> stream() {
        return broadcaster.ticks()
                .map(tick -> ServerSentEvent.<TelemetryTick>builder()
                        .id(Long.toString(tick.seq()))
                        .event("tick")
                        .data(tick)
                        .build());
    }

    /**
     * One snapshot frame, for clients that poll rather than subscribe.
     *
     * <p>Computed on demand rather than read out of the sink: the sink has no replay buffer, and
     * blocking on it would park an event-loop thread for no reason.
     */
    @GetMapping("/latest")
    public Mono<TelemetryTick> latest() {
        return telemetryService.buildTick(broadcaster.connectedClients())
                .onErrorReturn(new TelemetryTick(0L, System.currentTimeMillis(),
                        broadcaster.tickIntervalMillis(), broadcaster.connectedClients(),
                        java.time.Instant.now(), Map.of(), List.of()));
    }

    /** Broker health plus maritime connection state. */
    @GetMapping("/status")
    public Mono<Map<String, Object>> status() {
        return Mono.just(Map.of(
                "websocketPath", WebSocketConfig.TELEMETRY_PATH,
                "ssePath", "/api/v1/telemetry/stream",
                "domains", TelemetryWebSocketHandler.LAYER_DOMAINS,
                "tickIntervalMs", broadcaster.tickIntervalMillis(),
                "connectedClients", broadcaster.connectedClients(),
                "ticksEmitted", broadcaster.ticksEmitted(),
                "ticksFailed", broadcaster.ticksFailed(),
                "uptimeSeconds", telemetryService.uptimeSeconds(),
                "ais", telemetryService.aisStatus()));
    }
}
