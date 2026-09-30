package com.earthinformatics.explorer.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.earthinformatics.explorer.dto.DtoAssertions;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.Meta;
import com.earthinformatics.explorer.dto.SourceMeta;
import com.earthinformatics.explorer.dto.UpstreamResult;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The degradation contract.
 *
 * <p>Every upstream in this project is optional and rate limited, so "the provider failed" is a
 * normal operating state, not an exception. What matters is that a failure is visible: a payload
 * with zero features and {@code degraded=false} is a lie the dashboard cannot detect, and that is
 * exactly what the first live run produced for wildfires, tides and vegetation.
 */
class DegradationContractTest {

    @Nested
    @DisplayName("UpstreamResult.failure")
    class Failure {

        @Test
        @DisplayName("marks the payload meta degraded, not just the wrapper")
        void flagsThePayload() {
            UpstreamResult result = UpstreamResult.failure(
                    GeoJsonPayload.empty(Meta.live("NOAA CO-OPS")),
                    new SourceMeta("NOAA CO-OPS", "predictions", "https://example.invalid",
                            "public-domain"),
                    "upstream returned 500");

            assertThat(result.isDegraded()).isTrue();
            assertThat(result.error()).contains("500");
            // The flag the client actually sees.
            assertThat(result.payload().meta().degraded()).isTrue();
            assertThat(result.payload().meta().error()).contains("500");
        }

        @Test
        @DisplayName("keeps details that were already collected")
        void keepsDetails() {
            UpstreamResult result = UpstreamResult.failure(
                    GeoJsonPayload.empty(Meta.live("FIRMS")
                            .with("featureCount", 0)
                            .with("window", "24h")),
                    new SourceMeta("FIRMS", "area", "https://example.invalid", "public-domain"),
                    "401 Unauthorized");

            Meta meta = result.payload().meta();
            assertThat(meta.details()).containsEntry("window", "24h");
            assertThat(meta.degraded()).isTrue();
        }
    }

    @Nested
    @DisplayName("Meta")
    class MetaBehaviour {

        @Test
        @DisplayName("withDegraded marks degraded and records the reason")
        void withDegraded() {
            Meta meta = Meta.live("EONET").withDegraded("no events");

            assertThat(meta.degraded()).isTrue();
            assertThat(meta.error()).isEqualTo("no events");
            assertThat(meta.sources()).containsExactly("EONET");
        }

        @Test
        @DisplayName("withDegradedIfEither degrades a merged layer when one source failed")
        void mergeDegrades() {
            Meta healthy = Meta.live("USGS");
            Meta broken = Meta.live("EONET").withDegraded("timeout");

            assertThat(healthy.withDegradedIfEither(true).degraded()).isTrue();
            assertThat(healthy.withDegradedIfEither(false).degraded()).isFalse();
            assertThat(broken.withDegradedIfEither(false).degraded()).isTrue();
        }

        @Test
        @DisplayName("accepts null detail values")
        void nullDetailsAllowed() {
            Meta meta = Meta.live("test").with("heightMeters", null);

            assertThat(meta.details()).containsKey("heightMeters");
            assertThat(meta.details().get("heightMeters")).isNull();
        }

        @Test
        @DisplayName("metrics() tolerates null values, unlike Map.of")
        void metricsTolerateNull() {
            Map<String, Object> metrics = Meta.metrics("cells", 12, "maxWindSpeedMs", null);

            assertThat(metrics).containsEntry("cells", 12).containsKey("maxWindSpeedMs");
            assertThat(metrics.get("maxWindSpeedMs")).isNull();
        }

        @Test
        @DisplayName("metrics() rejects an odd number of arguments")
        void metricsRejectPairs() {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                    () -> Meta.metrics("cells", 1, "dangling"));
        }
    }

    @Nested
    @DisplayName("GeoJsonPayload")
    class Payload {

        @Test
        @DisplayName("withFeatures restamps the count")
        void restampsCount() {
            GeoJsonPayload payload = GeoJsonPayload.of(
                    List.of(GeoJson.point(0, 0)), Meta.live("test").with("featureCount", 1));

            GeoJsonPayload empty = payload.withFeatures(List.of());

            assertThat(empty.meta().details()).containsEntry("featureCount", 0);
            assertThat(empty.size()).isZero();
        }

        @Test
        @DisplayName("empty() keeps the metadata it was given")
        void emptyKeepsMeta() {
            GeoJsonPayload payload = GeoJsonPayload.empty(
                    Meta.live("AISStream.io").with("connectionState", "DEGRADED"));

            assertThat(payload.type()).isEqualTo(GeoJsonPayload.FEATURE_COLLECTION);
            assertThat(payload.features()).isEmpty();
            assertThat(payload.meta().details()).containsEntry("connectionState", "DEGRADED");
        }
    }

    @Nested
    @DisplayName("GeoJson.augment")
    class Augment {

        @Test
        @DisplayName("creates the properties member when the provider omits it")
        void createsProperties() {
            // EONET v3 features carry no properties member; withObject() throws on a missing
            // field, which surfaced as UnsupportedOperationException and an empty volcano layer.
            com.fasterxml.jackson.databind.node.ObjectNode feature =
                    JsonSupport.objectNode();
            feature.put("type", "Feature");
            feature.set("geometry", GeoJson.point(-71.378, -36.868));

            JsonNode augmented = GeoJson.augment(feature, "EONET_20710",
                    GeoJson.props("name", "Nevados del Chillan Volcano, Chile"));

            assertThat(augmented).isNotNull();
            assertThat(augmented.path("properties").path("name").asText())
                    .isEqualTo("Nevados del Chillan Volcano, Chile");
            assertThat(augmented.path("id").asText()).isEqualTo("EONET_20710");
        }

        @Test
        @DisplayName("overwrites provider values with normalised ones")
        void overwritesExtras() {
            com.fasterxml.jackson.databind.node.ObjectNode feature =
                    JsonSupport.objectNode();
            feature.put("type", "Feature");
            feature.set("geometry", GeoJson.point(1, 2));
            feature.set("properties", JsonSupport.objectNode().put("name", "unnamed"));

            JsonNode augmented = GeoJson.augment(feature, "id", GeoJson.props("name", "Telica"));

            assertThat(augmented.path("properties").path("name").asText()).isEqualTo("Telica");
        }

        @Test
        @DisplayName("rejects features without a usable geometry")
        void rejectsGeometrylessFeatures() {
            com.fasterxml.jackson.databind.node.ObjectNode feature =
                    JsonSupport.objectNode();
            feature.put("type", "Feature");

            assertThat(GeoJson.augment(feature, "id", GeoJson.props("a", 1))).isNull();
        }
    }

    @Test
    @DisplayName("every response shape is a strict GeoJSON FeatureCollection")
    void payloadShape() {
        DtoAssertions.assertFeatureCollectionShape(
                GeoJsonPayload.of(List.of(GeoJson.feature("a", 10, 20,
                        GeoJson.props("type", "test"))), Meta.live("test")));
    }
}
