package com.earthinformatics.explorer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cache.annotation.EnableCaching;

/**
 * Entry point for the Earth Informatics Explorer back-office.
 *
 * <p>The application is a fully non-blocking Spring WebFlux service that fronts a set of
 * free, public planetary data providers and re-publishes them as a single, uniform
 * GeoJSON + WebSocket API. Every upstream call is rate-limited and cached, so the
 * public providers are polled an order of magnitude less often than the globe
 * requests data.
 *
 * <p>Layering:
 * <pre>
 *   controller  -&gt; service -&gt; client -&gt; upstream provider
 *                       |                       (WebClient / ReactorNettyWebSocketClient)
 *                       +-&gt; @Cacheable (Caffeine)
 *   realtime    -&gt; TelemetryBroadcaster -&gt; WebSocketHandler (/ws/telemetry)
 * </pre>
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableCaching
@Slf4j
public class EarthInformaticsExplorerApplication {

    public static void main(String[] args) {
        SpringApplication.run(EarthInformaticsExplorerApplication.class, args);
    }
}
