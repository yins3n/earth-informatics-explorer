package com.earthinformatics.explorer.dto;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

/**
 * A GeoJSON {@code FeatureCollection} as emitted by the REST layer.
 *
 * <p>Each entry of {@code features} is a complete GeoJSON {@code Feature} node
 * ({@code {"type":"Feature","id":...,"geometry":{...},"properties":{...}}}), which is what
 * CesiumJS' {@code GeoJsonDataSource} and every mainstream GIS tool expects.
 *
 * <p>The record is immutable and therefore safe to hand straight out of a Caffeine cache
 * without defensive copying on every hit.
 */
public record GeoJsonPayload(String type, List<JsonNode> features, Meta meta) {

    public static final String FEATURE_COLLECTION = "FeatureCollection";

    public GeoJsonPayload {
        features = features == null ? List.of() : List.copyOf(features);
        // The count is derived, never supplied. Stamping it in the canonical constructor means
        // meta can never disagree with the collection, no matter which builder order a caller
        // uses - the earlier alternative let withMeta overwrite the count with a stale value.
        meta = meta == null ? Meta.live("unknown") : meta.with("featureCount", features.size());
    }

    public static GeoJsonPayload of(List<JsonNode> features, Meta meta) {
        return new GeoJsonPayload(FEATURE_COLLECTION, features, meta);
    }

    public static GeoJsonPayload empty(Meta meta) {
        return new GeoJsonPayload(FEATURE_COLLECTION, List.of(), meta);
    }

    /** Replaces the payload body; the count follows automatically via the constructor. */
    public GeoJsonPayload withFeatures(List<JsonNode> newFeatures) {
        return new GeoJsonPayload(FEATURE_COLLECTION, newFeatures, meta);
    }

    public GeoJsonPayload withMeta(Meta newMeta) {
        return new GeoJsonPayload(type, features, newMeta);
    }

    public int size() {
        return features.size();
    }
}
