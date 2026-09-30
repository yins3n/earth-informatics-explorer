package com.earthinformatics.explorer.config;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * Sets caching headers for the dashboard's own static files.
 *
 * <p>Spring's static resource handling sends {@code Last-Modified} but no {@code Cache-Control}.
 * Without an explicit policy a browser is free to apply heuristic freshness - typically a fraction
 * of the age of the file - and reuse a cached copy without asking. That is harmless for a
 * third-party library but not for files that change on every deploy: a browser can end up running
 * an old {@code app.js} against a new {@code index.html}, which fails in ways that look like
 * application bugs rather than a stale cache.
 *
 * <p>So the dashboard's own HTML, JavaScript and CSS are sent with {@code no-cache}: the browser
 * may keep them but must revalidate, which usually costs one cheap 304 and guarantees the browser
 * runs the code that matches the page. The vendored Cesium assets are the opposite case - they are
 * version-pinned inside the jar and never change for a given build - so they are marked immutable
 * and stay in the browser cache, which matters because {@code Cesium.js} alone is 5 MB.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class DashboardCacheFilter implements WebFilter {

    private static final String DASHBOARD_CACHE = "no-cache";
    private static final String VENDORED_CACHE = "public, max-age=31536000, immutable";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().pathWithinApplication().value();
        if (exchange.getRequest().getMethod() == org.springframework.http.HttpMethod.GET
                && isCacheable(path)) {
            exchange.getResponse().getHeaders().setCacheControl(
                    path.startsWith("/cesium/") ? VENDORED_CACHE : DASHBOARD_CACHE);
        }
        return chain.filter(exchange);
    }

    /** Only the files the dashboard itself serves; API and telemetry responses are left alone. */
    private static boolean isCacheable(String path) {
        return path.equals("/")
                || path.equals("/index.html")
                || path.startsWith("/js/")
                || path.startsWith("/css/")
                || path.startsWith("/cesium/");
    }
}
