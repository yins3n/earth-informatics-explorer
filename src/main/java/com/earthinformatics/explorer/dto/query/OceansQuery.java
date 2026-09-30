package com.earthinformatics.explorer.dto.query;

import com.earthinformatics.explorer.util.Geo;
import java.util.Locale;

/**
 * Parameters for the oceans endpoints.
 *
 * @param dataset  {@code tides}, {@code currents}, {@code sst} or {@code all}.
 * @param gridStep Lattice resolution in degrees for the sampled fields.
 * @param bbox     Viewport filter applied after the cache read.
 */
public record OceansQuery(String dataset, double gridStep, Geo.BBox bbox) {

    public OceansQuery {
        dataset = dataset == null || dataset.isBlank() ? "all"
                : dataset.trim().toLowerCase(Locale.ROOT);
    }

    public String cacheKey() {
        return "oceans:" + dataset + ":" + gridStep;
    }

    public boolean wantsTides() {
        return dataset.equals("all") || dataset.equals("tides");
    }

    public boolean wantsCurrents() {
        return dataset.equals("all") || dataset.equals("currents");
    }

    public boolean wantsSeaSurfaceTemperature() {
        return dataset.equals("all") || dataset.equals("sst");
    }
}
