package com.earthinformatics.explorer.dto.query;

import com.earthinformatics.explorer.util.Geo;

/**
 * Parameters for the biosphere endpoints.
 *
 * @param dataset      {@code wildfires} or {@code vegetation}.
 * @param days         FIRMS look-back window; {@code 0} requests the most recent satellite pass.
 * @param minConfidence FIRMS confidence rank floor (0 any, 1 low, 2 nominal, 3 high).
 * @param gridStep     NDVI lattice resolution in degrees.
 * @param bbox         Viewport filter applied after the cache read.
 */
public record BiosphereQuery(String dataset, int days, int minConfidence, double gridStep,
        Geo.BBox bbox) {

    public BiosphereQuery {
        dataset = dataset == null || dataset.isBlank() ? "wildfires"
                : dataset.trim().toLowerCase(java.util.Locale.ROOT);
        days = Math.max(0, Math.min(days, 10));
        minConfidence = Math.max(0, Math.min(minConfidence, 3));
    }

    public String wildfireCacheKey() {
        return "biosphere:wildfires:" + days;
    }

    public String vegetationCacheKey() {
        return "biosphere:vegetation:" + gridStep;
    }
}
