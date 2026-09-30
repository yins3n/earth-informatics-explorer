package com.earthinformatics.explorer.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

/**
 * Single-flight de-duplication for reactive pipelines.
 *
 * <p>Why this exists: {@code @Cacheable} only protects against <em>sequential</em> cache hits.
 * A globe that opens ten concurrent requests for the same tile on a cold cache produces ten
 * simultaneous upstream calls - exactly the thundering herd that earns an IP ban from a free
 * API. Single-flight collapses identical in-flight work into a single subscription.
 *
 * <p>Implementation is a lock-free {@link ConcurrentHashMap} of in-flight publishers.
 * {@code computeIfAbsent} gives atomic "return existing or install new" semantics, so the
 * losing thread simply adopts the winner's publisher. The operator removes its own entry on
 * termination, keeping the map proportional to genuinely concurrent work rather than to cache
 * size.
 *
 * <p>Thread-safety: safe. Blocking is never used - a thread that loses the race returns the
 * shared publisher without touching it, so no event-loop thread parks.
 *
 * <p>Companion measure: every {@code @Cacheable} method terminates its pipeline with
 * {@code Mono.cache()}, which makes a cached entry replayable to any number of subscribers
 * without re-issuing the upstream HTTP call.
 */
public final class SingleFlight {

    private final ConcurrentHashMap<String, Mono<?>> inFlight = new ConcurrentHashMap<>();
    private final int maxTracked;

    public SingleFlight() {
        this(512);
    }

    public SingleFlight(int maxTracked) {
        this.maxTracked = Math.max(16, maxTracked);
    }

    /**
     * Runs {@code supplier} unless an equivalent call is already in flight, in which case the
     * in-flight publisher is shared with the caller.
     *
     * @param key      Identity of the work, e.g. {@code tectonics:earthquakes:86400}.
     * @param supplier Zero-argument factory, invoked at most once per key at a time.
     */
    public <T> Mono<T> execute(String key, Supplier<Mono<T>> supplier) {
        if (inFlight.size() > maxTracked) {
            // Safety valve: under pathological load we would rather run unguarded than grow unbounded.
            return supplier.get();
        }
        // The reference lets doFinally remove exactly the instance it installed, so a late
        // termination signal can never evict a newer entry for the same key.
        AtomicReference<Mono<?>> installed = new AtomicReference<>();
        Mono<T> candidate = Mono.defer(supplier).doFinally(signal -> {
            Mono<?> self = installed.get();
            if (self != null) {
                inFlight.remove(key, self);
            }
        });
        installed.set(candidate);
        @SuppressWarnings("unchecked")
        Mono<T> shared = (Mono<T>) inFlight.computeIfAbsent(key, ignored -> candidate);
        return shared;
    }

    /** Memoising helper for short chains that do not warrant map bookkeeping. */
    public <T> Mono<T> once(Mono<T> source) {
        return source.cache();
    }

    /** Diagnostics for {@code /api/v1/system/caches}. */
    public int inFlightCount() {
        return inFlight.size();
    }

    /** Test seam. */
    public void reset() {
        inFlight.clear();
    }
}
