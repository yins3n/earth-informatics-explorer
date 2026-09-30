package com.earthinformatics.explorer.dto;

import static org.assertj.core.api.Assertions.assertThat;

import com.earthinformatics.explorer.util.GeoJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The one invariant every layer of the API depends on: {@code meta.details.featureCount} always
 * equals the number of features actually in the collection.
 *
 * <p>This has bitten four times, always the same way. {@link GeoJsonPayload#withFeatures} restamps
 * the count from the list it is handed, but {@link GeoJsonPayload#withMeta} takes a meta wholesale
 * - so writing {@code payload.withFeatures(filtered).withMeta(payload.meta()...)} silently
 * restores the <em>pre</em>-filter count. Nothing throws; the payload is just quietly wrong, and
 * the only way to notice is to compare a number in the metadata against the array you were sent.
 *
 * <p>These tests pin both the DTO behaviour and the call ordering the services depend on.
 */
class FeatureCountInvariantTest {

    private static JsonNode feature(String id, double lon, double lat) {
        return GeoJson.feature(id, lon, lat, GeoJson.props("type", "t"));
    }

    private static List<JsonNode> features(int count) {
        List<JsonNode> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(feature("f" + i, i, i));
        }
        return list;
    }

    private static void assertCountMatches(GeoJsonPayload payload) {
        Object declared = payload.meta().details().get("featureCount");
        assertThat(declared)
                .as("featureCount must be present in details")
                .isNotNull();
        assertThat(declared)
                .as("declared featureCount must equal the returned collection size")
                .isEqualTo(payload.features().size());
    }

    @Test
    @DisplayName("withFeatures restamps the count from the list it is given")
    void withFeaturesRestamps() {
        GeoJsonPayload payload = GeoJsonPayload.of(features(10),
                Meta.live("USGS").with("featureCount", 10));

        assertCountMatches(payload.withFeatures(features(3)));
    }

    @Test
    @DisplayName("applying withMeta before withFeatures keeps the count honest")
    void withMetaThenWithFeaturesIsSafe() {
        GeoJsonPayload payload = GeoJsonPayload.of(features(10),
                Meta.live("USGS").with("featureCount", 10).with("viewport", "global"));

        // The ordering the services use after a bbox or cap filter.
        GeoJsonPayload filtered = payload
                .withMeta(payload.meta().with("viewport", "BBox[west=-1.0, south=0.0, "
                        + "east=1.0, north=1.0]").with("truncated", true))
                .withFeatures(features(2));

        assertCountMatches(filtered);
        assertThat(filtered.meta().details())
                .as("the viewport detail must survive the restamp")
                .containsEntry("viewport", "BBox[west=-1.0, south=0.0, east=1.0, north=1.0]")
                .containsEntry("truncated", true);
    }

    @Test
    @DisplayName("the builder order no longer matters")
    void bothOrderingsAreSafe() {
        GeoJsonPayload payload = GeoJsonPayload.of(features(10),
                Meta.live("USGS").with("featureCount", 10));

        // This ordering used to be the bug: withMeta took a meta wholesale and restored the
        // pre-filter count. The count is now derived in the canonical constructor, so neither
        // order can desynchronise it - asserted both ways so a refactor cannot reintroduce it.
        GeoJsonPayload featuresFirst = payload
                .withFeatures(features(2))
                .withMeta(payload.meta().with("viewport", "global"));
        GeoJsonPayload metaFirst = payload
                .withMeta(payload.meta().with("viewport", "global"))
                .withFeatures(features(2));

        assertCountMatches(featuresFirst);
        assertCountMatches(metaFirst);
        assertThat(featuresFirst.features()).isEqualTo(metaFirst.features());
    }

    @Test
    @DisplayName("a capped page reports what it returns and flags the truncation")
    void cappedPageIsHonest() {
        GeoJsonPayload payload = GeoJsonPayload.of(features(12_221),
                Meta.live("OpenSky Network").with("featureCount", 12_221));
        List<JsonNode> capped = payload.features().subList(0, 8_000);

        GeoJsonPayload page = payload
                .withMeta(payload.meta().with("matched", 12_221)
                        .with("maxResults", 8_000)
                        .with("truncated", true))
                .withFeatures(List.copyOf(capped));

        assertThat(page.features()).hasSize(8_000);
        assertCountMatches(page);
        assertThat(page.meta().details())
                .containsEntry("truncated", true)
                .as("matched describes what was available, featureCount what is here")
                .containsEntry("matched", 12_221);
    }

    @Test
    @DisplayName("an empty degraded collection still declares zero")
    void emptyCollectionDeclaresZero() {
        GeoJsonPayload empty = GeoJsonPayload.of(List.of(),
                Meta.live("NASA FIRMS").withDegraded("401 unauthorized"));

        assertCountMatches(empty);
        assertThat(empty.meta().degraded()).isTrue();
    }
    @Test
    @DisplayName("list properties become real JSON arrays, not Java toString")
    void listPropertiesSerialiseAsArrays() {
        // The original bug: a List fell through to String.valueOf, so EONET published
        // "\"[Volcanoes]\"" - a string a client must re-parse to recover the array.
        GeoJsonPayload payload = GeoJsonPayload.of(
                List.of(GeoJson.feature("f1", 1, 1, new java.util.LinkedHashMap<>(java.util.Map.of(
                        "categoryTitles", java.util.List.of("Volcanoes", "Earthquakes"),
                        "sourceIds", java.util.List.of("SIVolcano", "GVP"))))),
                Meta.live("NASA EONET"));

        JsonNode properties = payload.features().get(0).path("properties");
        assertThat(properties.path("categoryTitles").isArray())
                .as("a list property must be a JSON array")
                .isTrue();
        assertThat(properties.path("categoryTitles"))
                .as("array contents must survive intact")
                .hasSize(2);
        assertThat(properties.path("categoryTitles").get(0).asText()).isEqualTo("Volcanoes");
        assertThat(properties.path("sourceIds").get(1).asText()).isEqualTo("GVP");
    }

    @Test
    @DisplayName("nested collections and scalars still convert correctly")
    void nestedValuesConvert() {
        ObjectNode properties = GeoJson.propertiesNode(new java.util.LinkedHashMap<>(java.util.Map.of(
                "scalar", "text",
                "flag", true,
                "whole", 7L,
                "fraction", 1.5d,
                "list", java.util.List.of("a", "b"),
                "numbers", java.util.List.of(1, 2, 3),
                "map", java.util.Map.of("k", "v"))));

        assertThat(properties.path("scalar").asText()).isEqualTo("text");
        assertThat(properties.path("flag").isBoolean()).isTrue();
        assertThat(properties.path("whole").asLong()).isEqualTo(7L);
        assertThat(properties.path("whole").isIntegralNumber())
                .as("whole numbers stay integral so ids and counts do not gain a .0")
                .isTrue();
        assertThat(properties.path("fraction").asDouble()).isEqualTo(1.5d);
        assertThat(properties.path("list").isArray()).isTrue();
        assertThat(properties.path("numbers").get(2).asInt()).isEqualTo(3);
        assertThat(properties.path("map").path("k").asText()).isEqualTo("v");
    }
}
