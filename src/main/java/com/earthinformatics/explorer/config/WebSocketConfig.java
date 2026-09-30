package com.earthinformatics.explorer.config;

import com.earthinformatics.explorer.realtime.TelemetryWebSocketHandler;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

/**
 * Publishes the telemetry WebSocket.
 *
 * <p>Spring's {@code WebSocketService} resolves the handler for an upgrade request through a
 * {@link HandlerMapping}. Registering the mapping explicitly (instead of relying on annotation
 * scanning) pins the path, makes it greppable, and guarantees the mapping is consulted before the
 * annotated controllers so an upgrade request is never mistaken for an HTTP GET.
 *
 * <p>The inbound message ceiling is not set here: in WebFlux the handshake decoder is shared
 * with HTTP/2 and is configured through {@code spring.codec.max-inbound-message-size}, which
 * {@code application.yml} derives from {@code explorer.websocket.max-inbound-message-size}.
 */
@Configuration(proxyBeanMethods = false)
public class WebSocketConfig {

    /** Canonical telemetry endpoint, also advertised by the REST catalog. */
    public static final String TELEMETRY_PATH = "/ws/telemetry";

    @Bean
    public HandlerMapping telemetryHandlerMapping(TelemetryWebSocketHandler handler) {
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setUrlMap(Map.of(TELEMETRY_PATH, handler));
        // Ahead of RequestMappingHandlerMapping (order 0) so the upgrade wins.
        mapping.setOrder(-1);
        return mapping;
    }

    @Bean
    public WebSocketHandlerAdapter webSocketHandlerAdapter() {
        return new WebSocketHandlerAdapter();
    }
}
