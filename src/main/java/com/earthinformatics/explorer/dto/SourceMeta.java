package com.earthinformatics.explorer.dto;

/**
 * Provenance of a single upstream document. Surfaced verbatim in the {@code meta} member of
 * every response so a viewer can always answer "where did this pixel come from, and when?".
 *
 * @param provider  Human readable organisation, e.g. {@code USGS}.
 * @param dataset   Feed/path within the provider, e.g. {@code all_day.geojson}.
 * @param endpoint  Absolute upstream URL (credentials redacted before it gets here).
 * @param license   Usage terms short form, e.g. {@code public-domain}.
 */
public record SourceMeta(String provider, String dataset, String endpoint, String license) {

    public static SourceMeta of(String provider, String dataset, String license) {
        return new SourceMeta(provider, dataset, null, license);
    }

    public SourceMeta withEndpoint(String endpoint) {
        return new SourceMeta(provider, dataset, endpoint, license);
    }
}
