package com.earthinformatics.explorer.config;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.SingleFlight;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Configuration;

/**
 * Caffeine cache topology.
 *
 * <p>Two managers with deliberately different policies:
 *
 * <table border="1">
 *   <caption>Cache tiers</caption>
 *   <tr><th>Tier</th><th>Beans</th><th>TTL</th><th>Contents</th></tr>
 *   <tr><td>tier 1</td><td>{@code cacheManager}</td><td>5-15 min</td>
 *       <td>bulk upstream documents</td></tr>
 *   <tr><td>tier 2</td><td>{@code shortLivedCacheManager}</td><td>30-90 s</td>
 *       <td>aircraft/vessel state vectors, pre-aggregated summaries</td></tr>
 * </table>
 *
 * <p>Why tier 1 uses plain {@code expireAfterWrite} rather than {@code refreshAfterWrite}:
 * asynchronous reload requires a {@code AsyncCacheLoader}, which interacts badly with the
 * reactive return type of our cached methods. We get the same protection - never block a user
 * on a slow provider - by (a) single-flight de-duplication on cold misses and (b) serving a
 * degraded-but-valid payload when a provider is down. Entries are also evicted eagerly on
 * degradation (see {@code CacheSupport}) so a blip is retried immediately instead of being
 * remembered for the whole TTL.
 */
@Configuration
@EnableCaching
public class CacheConfig {

    public static final String CACHE_MANAGER = "cacheManager";
    public static final String SHORT_LIVED_CACHE_MANAGER = "shortLivedCacheManager";

    /** Stable bean names, referenced from {@code @Cacheable(cacheManager = ...)}. */
    public static final String SHORT_LIVED = SHORT_LIVED_CACHE_MANAGER;

    /**
     * Primary manager for upstream documents: the tier that keeps free public providers alive.
     *
     * <p>Each cache is registered explicitly rather than through a shared default, because the
     * TTL is the actual rate-limit budget: 15 minutes across 14 tide stations is 1,344 calls/day
     * against NOAA's 500/day anonymous ceiling only if you actually visit the page - in practice
     * the TTL is far longer than any single client's session, so the steady-state rate is
     * "number of distinct cache entries / 15 minutes".
     */
    @Bean(name = CACHE_MANAGER)
    @Primary
    public CacheManager cacheManager(ExplorerProperties properties) {
        ExplorerProperties.Cache config = properties.cache();
        CaffeineCacheManager manager = baseManager();
        register(manager, Caches.EARTHQUAKES, config.defaultTtl(), 16);
        register(manager, Caches.VOLCANOES, config.defaultTtl(), 64);
        register(manager, Caches.WILDFIRES, ttlOf(config.defaultTtl(), 5), 16);
        register(manager, Caches.AIR_QUALITY, config.defaultTtl(), 32);
        register(manager, Caches.MARINE, config.defaultTtl(), 32);
        register(manager, Caches.FORECAST, ttlOf(config.defaultTtl(), 10), 32);
        register(manager, Caches.TIDES, config.defaultTtl(), 64);
        register(manager, Caches.WATER_TEMPERATURE, ttlOf(config.defaultTtl(), 30), 32);
        register(manager, Caches.NDVI, config.defaultTtl(), 8);
        manager.setCacheSpecification(defaultSpec(config.defaultTtl(), config.defaultMaxSize()));
        return manager;
    }

    /**
     * Manager for data that is stale within minutes of being wrong: aircraft state vectors,
     * vessel positions and the summary roll-ups that feed the WebSocket tick. A five minute TTL
     * on live traffic would be professionally indefensible - a 20 minute-old aircraft position is
     * not a live position - so this tier exists even though it sits outside the 5-15 minute
     * policy for bulk datasets.
     */
    @Bean(name = SHORT_LIVED_CACHE_MANAGER)
    public CacheManager shortLivedCacheManager(ExplorerProperties properties) {
        ExplorerProperties.Cache config = properties.cache();
        CaffeineCacheManager manager = baseManager();
        register(manager, Caches.FLIGHTS, Duration.ofSeconds(45), 8);
        register(manager, Caches.VESSELS, ttlOf(config.shortTtl(), 30), 8);
        register(manager, Caches.SUMMARIES, config.telemetryTtl(), 128);
        manager.setCacheSpecification(defaultSpec(config.shortTtl(), 128));
        return manager;
    }

    /**
     * Single-flight de-duplication shared by every service, sized from configuration.
     */
    @Bean
    public SingleFlight singleFlight(ExplorerProperties properties) {
        return new SingleFlight(properties.cache().singleFlight());
    }

    private CaffeineCacheManager baseManager() {
        CaffeineCacheManager manager = new CaffeineCacheManager();
        // Statistics feed /api/v1/system/caches and are what prove the cache is doing its job.
        manager.setCaffeine(Caffeine.newBuilder().recordStats());
        // Null values are never cached: a null result means "not available", and remembering it
        // for the whole TTL would blank a layer.
        manager.setAllowNullValues(false);
        // REQUIRED for reactive @Cacheable with Caffeine. Spring's reactive cache interceptor
        // resolves a hit through Cache.retrieve(key), and CaffeineCache implements that on top
        // of the AsyncCache view. Without this flag every reactive cache access throws
        // "No Caffeine AsyncCache available" - at runtime only, because it compiles fine.
        manager.setAsyncCacheMode(true);
        return manager;
    }

    /**
     * Registers one named cache.
     *
     * <p>The {@code buildAsync()} call is not cosmetic. {@code registerCustomCache(String,
     * Caffeine)} builds a {@code CaffeineCache} whose {@code asyncCache} field is left null,
     * while Spring's reactive cache interceptor resolves every hit through
     * {@code Cache.retrieve(key)} - implemented on that field. Registering the {@code AsyncCache}
     * returned by {@code buildAsync()} is what makes {@code @Cacheable Mono<...>} work at all;
     * without it the application starts cleanly and then throws "No Caffeine AsyncCache
     * available" on the first cached call. The two views share one underlying cache, so
     * statistics and expiry stay consistent between them.
     */
    private void register(CaffeineCacheManager manager, String name, Duration ttl, long maxSize) {
        manager.registerCustomCache(name, Caffeine.newBuilder()
                .recordStats()
                .expireAfterWrite(ttl)
                .expireAfterAccess(ttl)
                .maximumSize(maxSize)
                .buildAsync());
    }

    /** Falls back to the configured default when the requested tier is not shorter. */
    private static Duration ttlOf(Duration fallback, long minutes) {
        Duration candidate = Duration.ofMinutes(minutes);
        return candidate.compareTo(fallback) < 0 ? candidate : fallback;
    }

    private static String defaultSpec(Duration ttl, long maxSize) {
        long seconds = Math.max(1L, ttl.toSeconds());
        return "expireAfterWrite=" + seconds + "s,expireAfterAccess=" + seconds
                + "s,maximumSize=" + maxSize;
    }
}
