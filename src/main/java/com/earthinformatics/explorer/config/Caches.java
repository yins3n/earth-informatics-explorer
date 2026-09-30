package com.earthinformatics.explorer.config;

/**
 * Canonical cache names.
 *
 * <p>Names are dotted by upstream provider so that a cache dump immediately tells an operator
 * which third party a given bucket belongs to. {@link #DATASETS} are the long-TTL caches
 * (5-15 minutes, per the deployment policy); {@link #SHORT_LIVED} are the fast-moving
 * aggregates that must not look stale on the globe.
 */
public final class Caches {

    // ---- long TTL (5-15 min): heavy upstream documents, polled slowly ----
    public static final String EARTHQUAKES = "usgs.earthquakes";
    public static final String VOLCANOES = "eonet.volcanoes";
    public static final String WILDFIRES = "firms.wildfires";
    public static final String AIR_QUALITY = "openmeteo.air-quality";
    public static final String MARINE = "openmeteo.marine";
    public static final String FORECAST = "openmeteo.forecast";
    public static final String TIDES = "noaa.tides";
    public static final String WATER_TEMPERATURE = "noaa.water-temperature";
    public static final String NDVI = "ndvi.vegetation";
    // Layer catalogues change only when an operator republishes the server; a very long TTL is
    // safe and saves re-fetching multi-megabyte GetCapabilities documents on every request.
    public static final String WMS_CAPABILITIES = "ndvi.wms-capabilities";

    // ---- short TTL (30-90 s): kinetic layers and derived roll-ups ----
    public static final String FLIGHTS = "opensky.states";
    public static final String VESSELS = "ais.vessels";
    public static final String SUMMARIES = "explorer.summaries";

    private Caches() {
    }
}
