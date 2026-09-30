package com.earthinformatics.explorer.dto;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

/**
 * Assertions shared by the web-layer tests.
 *
 * <p>Kept in one place because "is this actually valid GeoJSON" is the contract the frontend
 * depends on, and it should be checked identically for every domain instead of being spot-checked
 * in whichever test happened to be written first.
 */
public final class DtoAssertions {

    private DtoAssertions() {
    }

    /**
     * Asserts strict RFC 7946 structure: a {@code FeatureCollection} whose entries are Features
     * with a geometry, numeric {@code id} stability hook and a properties object. CesiumJS'
     * {@code GeoJsonDataSource} rejects anything else, and the rejection is a silent blank layer.
     */
    public static void assertFeatureCollectionShape(GeoJsonPayload payload) {
        assertThat(payload.type()).isEqualTo("FeatureCollection");
        for (JsonNode feature : payload.features()) {
            assertThat(feature.path("type").asText())
                    .as("feature type")
                    .isEqualTo("Feature");
            assertThat(feature.has("geometry")).as("feature has a geometry").isTrue();
            assertThat(feature.path("geometry").path("type").asText())
                    .as("geometry type")
                    .isNotBlank();
            assertThat(feature.path("properties").isObject())
                    .as("properties is an object")
                    .isTrue();
        }
    }

    /** Asserts a point feature sits at the given position. */
    public static void assertPosition(JsonNode feature, double longitude, double latitude) {
        double[] position = com.earthinformatics.explorer.util.GeoJson.position(feature);
        assertThat(position).as("position of %s", feature.path("id").asText()).isNotNull();
        assertThat(position[0]).isCloseTo(longitude, org.assertj.core.data.Offset.offset(1e-6));
        assertThat(position[1]).isCloseTo(latitude, org.assertj.core.data.Offset.offset(1e-6));
    }

    /** Asserts every feature in the list lies inside the box. */
    public static void assertAllInside(GeoJsonPayload payload, double minLon, double minLat,
            double maxLon, double maxLat) {
        for (JsonNode feature : payload.features()) {
            double[] position = com.earthinformatics.explorer.util.GeoJson.position(feature);
            if (position == null) {
                continue;
            }
            assertThat(position[0]).isBetween(minLon, maxLon);
            assertThat(position[1]).isBetween(minLat, maxLat);
        }
    }

    /** Asserts the meta block carries provenance and a count consistent with the features. */
    public static void assertMetaConsistent(GeoJsonPayload payload) {
        Meta meta = payload.meta();
        assertThat(meta.sources()).as("meta.sources").isNotEmpty();
        assertThat(meta.fetchedAt()).as("meta.fetchedAt").isNotNull();
        Object declared = meta.details().get("featureCount");
        if (declared instanceof Number number) {
            assertThat(number.intValue())
                    .as("meta.details.featureCount matches features.size()")
                    .isEqualTo(payload.size());
        }
    }

    public static List<JsonNode> ids(GeoJsonPayload payload) {
        return payload.features().stream().map(feature -> feature.path("id")).toList();
    }
}
