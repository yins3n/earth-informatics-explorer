package com.earthinformatics.explorer.dto;

import java.time.Instant;

/**
 * Value type stored in the Caffeine caches.
 *
 * <p>Design note - the cached value <em>never fails</em>. Upstream errors are converted into a
 * degraded-but-valid payload so that (a) a transient 5xx from a provider cannot poison the cache
 * for the whole TTL and (b) the client always receives renderable GeoJSON instead of an error
 * document. Entries that come back degraded are evicted immediately by
 * {@code CacheSupport#evictIfDegraded}, so the very next caller retries upstream.
 *
 * @param payload        Feature collection to hand to the renderer.
 * @param source         Provenance of the document that produced the payload.
 * @param degraded       {@code true} when the payload was synthesised after an upstream failure.
 * @param error          Failure description when {@code degraded}, otherwise {@code null}.
 * @param upstreamMillis Wall time spent talking to the provider (excludes cache hits).
 * @param fetchedAt      When the upstream document was retrieved.
 */
public record UpstreamResult(
        GeoJsonPayload payload,
        SourceMeta source,
        boolean degraded,
        String error,
        long upstreamMillis,
        Instant fetchedAt) {

    public static UpstreamResult success(GeoJsonPayload payload, SourceMeta source,
            long upstreamMillis) {
        return new UpstreamResult(payload, source, false, null, upstreamMillis, Instant.now());
    }

    /**
     * Builds an empty, explicitly-flagged payload. The caller supplies the reason; the user
     * still sees a globe, just with an honest "source degraded" badge.
     *
     * <p>The payload's own {@code meta} is stamped degraded here rather than at each call site.
     * Doing it in one place matters: the flag reaches the client through {@code meta}, so a
     * service that forgot to mark it produced responses that claimed to be live while serving
     * nothing - the most damaging kind of silent failure this API can have.
     */
    public static UpstreamResult failure(GeoJsonPayload emptyPayload, SourceMeta source,
            String reason) {
        GeoJsonPayload flagged = emptyPayload.withMeta(
                emptyPayload.meta().withDegraded(reason));
        return new UpstreamResult(flagged, source, true, reason, 0L, Instant.now());
    }

    public boolean isDegraded() {
        return degraded;
    }
}
