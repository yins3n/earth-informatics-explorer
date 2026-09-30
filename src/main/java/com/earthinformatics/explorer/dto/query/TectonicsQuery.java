package com.earthinformatics.explorer.dto.query;

import com.earthinformatics.explorer.util.Geo;
import java.util.List;

/**
 * Parameters for the tectonics endpoints.
 *
 * <p>Note the deliberate split between {@link #cacheKey()} and the filter fields. Only the
 * upstream-shaping parameters take part in the cache key; the viewport, magnitude threshold and
 * result cap are applied <em>after</em> the cached document is read. Ten users panning ten
 * different viewports therefore share one cache entry and one upstream poll, which is the whole
 * point of the caching layer.
 *
 * @param feed         USGS feed, or {@code auto} for the configured default.
 * @param category     EONET category.
 * @param days         Look-back window for EONET events.
 * @param minMagnitude Discard earthquakes below this magnitude (applied post-cache).
 * @param maxResults   Hard cap on returned features (applied post-cache).
 * @param bbox         Viewport filter; {@code null} means the whole globe.
 */
public record TectonicsQuery(
        String feed,
        String category,
        int days,
        double minMagnitude,
        int maxResults,
        Geo.BBox bbox) {

    public TectonicsQuery {
        feed = feed == null || feed.isBlank() ? "auto" : feed.trim().toLowerCase(java.util.Locale.ROOT);
        category = category == null || category.isBlank() ? "volcanoes"
                : category.trim().toLowerCase(java.util.Locale.ROOT);
        minMagnitude = Math.max(-1d, Math.min(minMagnitude, 10d));
        maxResults = Math.max(1, Math.min(maxResults, 20_000));
    }

    /** Static shape of the upstream call - the only part that may influence a cache entry. */
    public String cacheKey() {
        return "tectonics:eq:" + feed + ":" + maxResults;
    }

    public String volcanoCacheKey() {
        return "tectonics:volcanoes:" + category + ":" + days + ":" + maxResults;
    }

    public boolean hasViewport() {
        return bbox != null;
    }

    public List<Geo.BBox> bounds() {
        return bbox == null ? List.of() : List.of(bbox);
    }
}
