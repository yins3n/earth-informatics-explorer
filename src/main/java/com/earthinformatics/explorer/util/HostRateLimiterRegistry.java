package com.earthinformatics.explorer.util;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * Registry of per-host token buckets.
 *
 * <p>A bucket is created lazily on first sight of a host, so adding a new upstream source
 * needs no configuration change. Hosts are matched case-insensitively and ignore the port so
 * that http/https variants of the same provider share a budget.
 */
@Component
public class HostRateLimiterRegistry {

    private final Map<String, TokenBucketRateLimiter> limiters = new ConcurrentHashMap<>();
    private final double tokensPerSecond;
    private final double burst;

    public HostRateLimiterRegistry(ExplorerProperties properties) {
        this.tokensPerSecond = properties.upstreams().resilience().tokensPerSecond();
        this.burst = properties.upstreams().resilience().burst();
    }

    public TokenBucketRateLimiter forHost(String host) {
        if (host == null || host.isBlank()) {
            return new TokenBucketRateLimiter(tokensPerSecond, burst);
        }
        return limiters.computeIfAbsent(normalise(host),
                ignored -> new TokenBucketRateLimiter(tokensPerSecond, burst));
    }

    public int trackedHosts() {
        return limiters.size();
    }

    public Map<String, Double> snapshot() {
        Map<String, Double> snapshot = new TreeMap<>();
        limiters.forEach((host, limiter) -> snapshot.put(host, limiter.availablePermits()));
        return snapshot;
    }

    private static String normalise(String host) {
        String value = host.toLowerCase(Locale.ROOT);
        int colon = value.indexOf(':');
        return colon > 0 ? value.substring(0, colon) : value;
    }
}
