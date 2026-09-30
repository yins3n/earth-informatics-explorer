package com.earthinformatics.explorer.dto.query;

import com.earthinformatics.explorer.util.Geo;
import java.util.Locale;

/**
 * Parameters for the atmospherics endpoints (wind field, air quality, surface weather).
 *
 * @param dataset  {@code forecast}, {@code air-quality} or {@code all}.
 * @param gridStep Lattice resolution in degrees.
 * @param metric   Optional metric filter: {@code wind}, {@code aqi} or {@code null} for both.
 * @param bbox     Viewport filter applied after the cache read.
 */
public record AtmosphericsQuery(String dataset, double gridStep, String metric, Geo.BBox bbox) {

    public AtmosphericsQuery {
        dataset = dataset == null || dataset.isBlank() ? "all"
                : dataset.trim().toLowerCase(Locale.ROOT);
        metric = metric == null || metric.isBlank() ? null : metric.trim().toLowerCase(Locale.ROOT);
    }

    public String cacheKey() {
        return "atmospherics:" + dataset + ":" + gridStep;
    }

    public boolean wantsWind() {
        return metric == null || metric.equals("wind");
    }

    public boolean wantsAirQuality() {
        return metric == null || metric.equals("aqi");
    }
}
