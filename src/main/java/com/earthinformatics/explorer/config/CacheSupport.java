package com.earthinformatics.explorer.config;

import com.earthinformatics.explorer.dto.UpstreamResult;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

/**
 * Cache maintenance helpers that annotation processing cannot express.
 *
 * <p>The central trick of this service: a cached value is allowed to be <em>degraded</em>
 * (empty but valid), yet such an entry must not be allowed to live for the full TTL - a
 * ten second USGS hiccup should not blank the globe for fifteen minutes. Every service
 * therefore funnels its result through {@link #evictIfDegraded(String, Object, UpstreamResult)}
 * after the cache lookup, which drops the entry the moment we learn it was a fallback.
 *
 * <p>Doing this programmatically rather than with {@code @CacheEvict(condition = ...)} is
 * deliberate: {@code @CacheEvict}'s SpEL sees the reactive wrapper, not the resolved value, so
 * the condition cannot inspect {@link UpstreamResult#degraded()}.
 */
@Component
@Slf4j
public class CacheSupport {

    private final CacheManager primary;
    private final CacheManager shortLived;

    public CacheSupport(@Qualifier(CacheConfig.CACHE_MANAGER) CacheManager primary,
            @Qualifier(CacheConfig.SHORT_LIVED_CACHE_MANAGER) CacheManager shortLived) {
        this.primary = primary;
        this.shortLived = shortLived;
    }

    /** Drops the entry backing {@code cacheName/key} when the result is a degraded fallback. */
    public void evictIfDegraded(String cacheName, Object key, UpstreamResult result) {
        if (result != null && result.degraded()) {
            evict(cacheName, key);
            log.warn("Evicted degraded cache entry {}/{} -> {}", cacheName, key, result.error());
        }
    }

    public void evict(String cacheName, Object key) {
        Cache cache = cache(cacheName);
        if (cache != null) {
            cache.evictIfPresent(key);
        }
    }

    public void clear(String cacheName) {
        Cache cache = cache(cacheName);
        if (cache != null) {
            cache.clear();
        }
    }

    /**
     * Per-cache hit/miss/eviction statistics. Caffeine exposes these as a string-keyed map on
     * the native cache; we surface the fields an operator actually acts on.
     */
    public Map<String, Object> statistics() {
        Map<String, Object> report = new LinkedHashMap<>();
        collect(primary, report);
        collect(shortLived, report);
        return report;
    }

    private void collect(CacheManager manager, Map<String, Object> report) {
        for (String name : manager.getCacheNames()) {
            Cache cache = manager.getCache(name);
            if (cache == null) {
                continue;
            }
            Object nativeCache = cache.getNativeCache();
            if (nativeCache instanceof com.github.benmanes.caffeine.cache.Cache<?, ?> caffeine) {
                report.put(name, Map.of(
                        "size", caffeine.estimatedSize(),
                        "hits", caffeine.stats().hitCount(),
                        "misses", caffeine.stats().missCount(),
                        "evictions", caffeine.stats().evictionCount(),
                        "hitRate", caffeine.stats().hitRate(),
                        "loadFailures", caffeine.stats().loadFailureCount()));
            } else {
                report.put(name, Map.of("implementation", nativeCache.getClass().getName()));
            }
        }
    }

    private Cache cache(String name) {
        Cache cache = primary.getCache(name);
        return cache != null ? cache : shortLived.getCache(name);
    }
}
