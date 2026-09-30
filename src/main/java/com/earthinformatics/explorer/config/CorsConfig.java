package com.earthinformatics.explorer.error;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsWebFilter;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

/**
 * CORS for the dashboard.
 *
 * <p>The globe is normally served from the same origin ({@code http://localhost:8080}), so CORS
 * is only needed when the CesiumJS front-end is hosted separately - a common split once the
 * static bundle moves behind a CDN. Origins are configurable and {@code *} is supported for
 * local development.
 */
@Configuration
public class CorsConfig {

    @Bean
    public CorsWebFilter corsWebFilter(ExplorerProperties properties) {
        ExplorerProperties.Api api = properties.api();
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        if (api.corsEnabled()) {
            CorsConfiguration configuration = new CorsConfiguration();
            configuration.setAllowedOriginPatterns(api.allowedOrigins());
            configuration.setAllowedMethods(List.of("GET", "HEAD", "OPTIONS"));
            // The WS handshake is a plain HTTP upgrade; no credentials are used.
            configuration.setAllowedHeaders(List.of("*"));
            configuration.setMaxAge(3600L);
            source.registerCorsConfiguration("/api/**", configuration);
        }
        return new CorsWebFilter(source);
    }
}
