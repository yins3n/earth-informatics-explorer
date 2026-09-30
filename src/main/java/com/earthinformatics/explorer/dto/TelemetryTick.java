package com.earthinformatics.explorer.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Single frame broadcast on {@code /ws/telemetry}.
 *
 * @param seq         Monotonic frame counter, lets the client detect dropped frames.
 * @param serverTime  Broadcast instant (epoch millis) used for round-trip latency.
 * @param tickIntervalMs Nominal interval so the client can animate between frames.
 * @param clients     Number of subscribers currently attached.
 * @param summaries   Per-domain roll-up keyed by layer domain id.
 * @param latest      Freshest features across domains, flattened for the pulse overlay.
 */
public record TelemetryTick(
        long seq,
        long serverTime,
        long tickIntervalMs,
        int clients,
        Instant generatedAt,
        Map<String, DomainSummary> summaries,
        List<TelemetryEvent> latest) {

    /**
     * One "something just happened" item, trimmed to the fields the globe needs.
     *
     * @param domain Layer domain this event came from, so a client that subscribed to a subset
     *               can be sent a subset. Derived here rather than guessed by the client from
     *               {@code type}, which would couple the wire format to provider vocabulary.
     */
    public record TelemetryEvent(
            String domain,
            String type,
            String id,
            double latitude,
            double longitude,
            double magnitude,
            String label,
            long timestamp) {
    }
}
