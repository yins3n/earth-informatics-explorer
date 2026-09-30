package com.earthinformatics.explorer.dto.query;

import com.earthinformatics.explorer.util.Geo;

/**
 * Parameters for the human-impact endpoints.
 *
 * @param bbox    Viewport filter; the OpenSky request itself is bounded by this box.
 * @param maxResults Cap on returned entities.
 * @param airborneOnly Drop aircraft reporting {@code onGround = true}.
 */
public record HumanImpactQuery(Geo.BBox bbox, int maxResults, boolean airborneOnly) {

    public HumanImpactQuery {
        maxResults = Math.max(1, Math.min(maxResults, 8_000));
    }

    /**
     * Global requests are cached once; a bounded request is a different upstream call and
     * therefore a different cache entry.
     */
    public String flightsCacheKey() {
        return bbox == null ? "human-impact:flights:global" + ":" + maxResults
                : "human-impact:flights:%s,%s,%s,%s:%d".formatted(
                bbox.west(), bbox.south(), bbox.east(), bbox.north(), maxResults);
    }

    public String vesselsCacheKey() {
        return "human-impact:vessels:ais";
    }
}
