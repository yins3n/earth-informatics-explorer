package com.earthinformatics.explorer.controller;

import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Helpers for assembling the composite "everything at once" endpoints.
 *
 * <p>The globe treats each domain as a single entity, so when a user enables a whole domain the
 * server returns one feature collection rather than several. That saves a round trip and avoids
 * a visible half-populated globe while the second request is still in flight.
 */
final class Payloads {

    private Payloads() {
    }

    /**
     * Concatenates feature collections, unioning sources and propagating degradation.
     *
     * <p>Degradation is intentionally pessimistic: a layer is only as trustworthy as its least
     * trustworthy contributor, so one degraded sub-source degrades the merged result and the
     * frontend banner tells the user what is missing.
     */
    static GeoJsonPayload merge(String domain, GeoJsonPayload first, GeoJsonPayload second) {
        List<JsonNode> features = new ArrayList<>(first.size() + second.size());
        features.addAll(first.features());
        features.addAll(second.features());

        List<String> sources = Stream.concat(
                        first.meta().sources().stream(), second.meta().sources().stream())
                .distinct()
                .toList();

        var meta = (first.meta().upstreamMillis() >= second.meta().upstreamMillis()
                        ? first.meta()
                        : second.meta())
                .withSources(sources)
                .with("domain", domain)
                .with("mergedSources", sources.size())
                // The inherited count describes only the sub-source it came from; restamping is
                // what stops a composite from claiming 232 features while returning 245.
                .withDegradedIfEither(first.meta().degraded() || second.meta().degraded());

        return GeoJsonPayload.of(features, meta);
    }

    /** Concatenates an arbitrary number of payloads in order. */
    static GeoJsonPayload mergeAll(String domain, GeoJsonPayload... payloads) {
        List<JsonNode> features = new ArrayList<>();
        java.util.Set<String> sources = new java.util.LinkedHashSet<>();
        boolean degraded = false;
        long slowest = 0;
        var meta = payloads.length == 0
                ? com.earthinformatics.explorer.dto.Meta.live("none")
                : payloads[0].meta();

        for (GeoJsonPayload payload : payloads) {
            features.addAll(payload.features());
            sources.addAll(payload.meta().sources());
            degraded |= payload.meta().degraded();
            slowest = Math.max(slowest, payload.meta().upstreamMillis());
        }

        return GeoJsonPayload.of(features, meta
                .withSources(List.copyOf(sources))
                .with("domain", domain)
                .withUpstreamMillis(slowest)
                .withDegradedIfEither(degraded));
    }
}
