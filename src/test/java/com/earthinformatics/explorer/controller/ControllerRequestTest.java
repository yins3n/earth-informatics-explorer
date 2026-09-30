package com.earthinformatics.explorer.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.earthinformatics.explorer.TestProperties;
import com.earthinformatics.explorer.client.AisStreamClient;
import com.earthinformatics.explorer.dto.DomainSummary;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.Meta;
import com.earthinformatics.explorer.dto.query.AtmosphericsQuery;
import com.earthinformatics.explorer.dto.query.BiosphereQuery;
import com.earthinformatics.explorer.dto.query.HumanImpactQuery;
import com.earthinformatics.explorer.dto.query.OceansQuery;
import com.earthinformatics.explorer.dto.query.TectonicsQuery;
import com.earthinformatics.explorer.error.InvalidRequestException;
import com.earthinformatics.explorer.service.AtmosphericsService;
import com.earthinformatics.explorer.service.BiosphereService;
import com.earthinformatics.explorer.service.HumanImpactService;
import com.earthinformatics.explorer.service.OceansService;
import com.earthinformatics.explorer.service.TectonicsService;
import com.earthinformatics.explorer.util.GeoJson;
import com.earthinformatics.explorer.util.JsonSupport;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Controller request translation, with the services stubbed out.
 *
 * <p>What matters at this layer is not the payload - the services own that - but the translation
 * from query string to a validated query record. Every range check is decided here, and it is the
 * difference between one client asking for a 0.001-degree lattice and the upstream rate limit that
 * takes the endpoint down for everyone else.
 *
 * <p>Note the validation contract: parameters are <em>rejected</em> with a 400, not clamped. A
 * clamp would hand the client a lattice it did not ask for and produce a subtly wrong map with no
 * signal that anything went wrong. WebFlux turns these thrown exceptions into the 400 responses,
 * so the tests assert the throw.
 */
class ControllerRequestTest {

    private static final Meta LIVE = Meta.live("USGS").with("featureCount", 1);

    private static GeoJsonPayload oneFeature() {
        return GeoJsonPayload.of(
                List.of(GeoJson.feature("eq-1", 10, 20,
                        GeoJson.props("type", "earthquake", "magnitude", 4.2))),
                LIVE);
    }

    private static <T> ArgumentCaptor<T> capture(Class<T> type) {
        return ArgumentCaptor.forClass(type);
    }

    @Nested
    @DisplayName("tectonics")
    class Tectonics {

        private final TectonicsService service = mock(TectonicsService.class);
        private final TectonicsController controller =
                new TectonicsController(service, TestProperties.defaults());

        @Test
        @DisplayName("a valid request reaches the service with the query intact")
        void acceptsValidRequest() {
            when(service.earthquakes(any())).thenReturn(Mono.just(oneFeature()));

            StepVerifier.create(controller.earthquakes("all_day", 4.5, 2000, "-10,-10,10,10"))
                    .expectNextCount(1)
                    .verifyComplete();

            ArgumentCaptor<TectonicsQuery> query = capture(TectonicsQuery.class);
            verify(service).earthquakes(query.capture());
            assertThat(query.getValue().feed()).isEqualTo("all_day");
            assertThat(query.getValue().minMagnitude()).isEqualTo(4.5);
            assertThat(query.getValue().maxResults()).isEqualTo(2000);
            assertThat(query.getValue().bbox()).isNotNull();
        }

        @Test
        @DisplayName("an absurd maxResults is rejected, not silently clamped")
        void rejectsOversizedMaxResults() {
            assertThatThrownBy(() -> controller.earthquakes(null, null, Integer.MAX_VALUE, null))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessageContaining("maxResults")
                    .hasMessageContaining("20000");

            verify(service, never()).earthquakes(any());
        }

        @Test
        @DisplayName("a malformed bbox is rejected instead of silently matching nothing")
        void rejectsMalformedBbox() {
            // A three-value box parses as west/south/east and would match no features at all,
            // which the client would read as an empty-but-healthy layer.
            assertThatThrownBy(() -> controller.earthquakes(null, null, 2000, "1,2,3"))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessageContaining("bbox");

            verify(service, never()).earthquakes(any());
        }

        @Test
        @DisplayName("an out-of-range magnitude is rejected with a usable message")
        void rejectsImpossibleMagnitude() {
            assertThatThrownBy(() -> controller.earthquakes(null, -99.0, 2000, null))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessageContaining("minMagnitude");

            verify(service, never()).earthquakes(any());
        }
    }

    @Nested
    @DisplayName("oceans")
    class Oceans {

        private final OceansService service = mock(OceansService.class);
        private final OceansController controller = new OceansController(service);

        @Test
        @DisplayName("a tide window NOAA cannot serve is rejected before the request goes out")
        void rejectsTideWindowBeyondNoaa() {
            assertThatThrownBy(() -> controller.tides(365_000, null))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessageContaining("days")
                    .hasMessageContaining("7");

            verify(service, never()).tides(any(), anyInt());
        }

        @Test
        @DisplayName("a valid tide window is passed through as-is")
        void passesThroughTideWindow() {
            when(service.tides(any(), anyInt())).thenReturn(Mono.just(oneFeature()));

            StepVerifier.create(controller.tides(2, null))
                    .expectNextCount(1)
                    .verifyComplete();

            verify(service).tides(any(OceansQuery.class), eq(2));
        }
    }

    @Nested
    @DisplayName("atmospherics")
    class Atmospherics {

        private final AtmosphericsService service = mock(AtmosphericsService.class);
        private final AtmosphericsController controller = new AtmosphericsController(service);

        @Test
        @DisplayName("a sub-degree lattice is rejected rather than attempted")
        void rejectsFineGridStep() {
            // At 0.001 degrees this is half a billion cells: the request would fan out into
            // thousands of chunk calls and take the endpoint down for everyone.
            assertThatThrownBy(() -> controller.wind(0.001, null))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessageContaining("gridStep");

            verify(service, never()).windAndWeather(any());
        }

        @Test
        @DisplayName("an over-coarse lattice is rejected too")
        void rejectsCoarseGridStep() {
            assertThatThrownBy(() -> controller.wind(5000, null))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessageContaining("gridStep");

            verify(service, never()).windAndWeather(any());
        }

        @Test
        @DisplayName("a lattice at the upper boundary is accepted")
        void acceptsBoundaryGridStep() {
            when(service.windAndWeather(any())).thenReturn(Mono.just(oneFeature()));

            StepVerifier.create(controller.wind(90, null))
                    .expectNextCount(1)
                    .verifyComplete();

            ArgumentCaptor<AtmosphericsQuery> query = capture(AtmosphericsQuery.class);
            verify(service).windAndWeather(query.capture());
            assertThat(query.getValue().gridStep()).isEqualTo(90d);
        }
    }

    @Nested
    @DisplayName("biosphere")
    class Biosphere {

        private final BiosphereService service = mock(BiosphereService.class);
        private final BiosphereController controller = new BiosphereController(service);

        @Test
        @DisplayName("a zero-day wildfire window means today, and is accepted")
        void acceptsZeroDayWindow() {
            when(service.wildfires(any())).thenReturn(Mono.just(oneFeature()));

            StepVerifier.create(controller.wildfires(0, 0, 2000, null))
                    .expectNextCount(1)
                    .verifyComplete();

            ArgumentCaptor<BiosphereQuery> query = capture(BiosphereQuery.class);
            verify(service).wildfires(query.capture());
            assertThat(query.getValue().days()).isZero();
        }

        @Test
        @DisplayName("a confidence outside the 0-3 FIRMS scale is rejected")
        void rejectsImpossibleConfidence() {
            assertThatThrownBy(() -> controller.wildfires(1, 900, 2000, null))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessageContaining("minConfidence");

            assertThatThrownBy(() -> controller.wildfires(1, 50, 2000, null))
                    .as("FIRMS confidence is a 0-3 scale, not a percentage")
                    .isInstanceOf(InvalidRequestException.class);

            verify(service, never()).wildfires(any());
        }

        @Test
        @DisplayName("a valid wildfire request reaches the service")
        void acceptsValidWildfireRequest() {
            when(service.wildfires(any())).thenReturn(Mono.just(oneFeature()));

            StepVerifier.create(controller.wildfires(1, 2, 2000, null))
                    .expectNextCount(1)
                    .verifyComplete();

            ArgumentCaptor<BiosphereQuery> query = capture(BiosphereQuery.class);
            verify(service).wildfires(query.capture());
            assertThat(query.getValue().days()).isEqualTo(1);
            assertThat(query.getValue().minConfidence()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("human impact")
    class Human {

        private final HumanImpactService service = mock(HumanImpactService.class);
        private final HumanImpactController controller =
                new HumanImpactController(service, mock(AisStreamClient.class));

        @Test
        @DisplayName("a vessel cap beyond the ceiling is rejected")
        void rejectsOversizedVesselCap() {
            assertThatThrownBy(() -> controller.vessels(null, Integer.MAX_VALUE))
                    .isInstanceOf(InvalidRequestException.class)
                    .hasMessageContaining("maxResults");

            verify(service, never()).vessels(any());
        }

        @Test
        @DisplayName("a truncated vessel page declares itself as truncated")
        void truncatedPageIsDeclared() {
            when(service.vessels(any())).thenReturn(Mono.just(GeoJsonPayload.empty(
                    Meta.live("AISStream.io").with("maxResults", 10).with("truncated", true))));

            StepVerifier.create(controller.vessels(null, 10))
                    .assertNext(payload -> assertThat(payload.meta().details())
                            .as("a capped page must not read as the whole picture")
                            .containsEntry("truncated", true))
                    .verifyComplete();

            ArgumentCaptor<HumanImpactQuery> query = capture(HumanImpactQuery.class);
            verify(service).vessels(query.capture());
            assertThat(query.getValue().maxResults()).isEqualTo(10);
        }
    }

    @Nested
    @DisplayName("degradation reporting")
    class Degradation {

        @Test
        @DisplayName("an unavailable domain reports itself rather than claiming health")
        void unavailableSummary() {
            DomainSummary degraded = DomainSummary.unavailable("oceans", "rate limited");

            assertThat(degraded.degraded()).isTrue();
            assertThat(degraded.count()).isZero();
        }

        @Test
        @DisplayName("a non-finite headline metric is projected to null, not emitted as NaN")
        void nonFiniteHeadlineIsNull() throws Exception {
            String json = JsonSupport.mapper()
                    .writeValueAsString(DomainSummary.empty("biosphere"));

            // NaN is not a JSON number; a strict parser rejects the whole document.
            assertThat(json).doesNotContain("NaN").contains("\"maxValue\":null");
        }

        @Test
        @DisplayName("an empty collection still carries provenance")
        void emptyCollectionKeepsMeta() {
            GeoJsonPayload empty = GeoJsonPayload.empty(
                    Meta.live("AISStream.io").with("connectionState", "DEGRADED"));

            assertThat(empty.type()).isEqualTo("FeatureCollection");
            assertThat(empty.features()).isEmpty();
            assertThat(empty.meta().details()).containsEntry("connectionState", "DEGRADED");
        }
    }

    @Nested
    @DisplayName("composite merges")
    class Merges {

        @Test
        @DisplayName("merging restamps the count and unions sources")
        void mergeCombinesLayers() {
            GeoJsonPayload first = GeoJsonPayload.of(
                    List.of(GeoJson.feature("a", 1, 1, GeoJson.props("type", "a"))),
                    Meta.live("USGS").with("featureCount", 1));
            GeoJsonPayload second = GeoJsonPayload.of(
                    List.of(GeoJson.feature("b", 2, 2, GeoJson.props("type", "b"))),
                    Meta.live("NASA EONET").with("featureCount", 1));

            GeoJsonPayload merged = Payloads.merge("tectonics", first, second);

            assertThat(merged.features()).hasSize(2);
            assertThat(merged.meta().sources())
                    .as("a composite layer must credit both upstreams")
                    .contains("USGS", "NASA EONET");
            assertThat(merged.meta().details())
                    .as("the inherited count describes one sub-source; the merged one must not")
                    .containsEntry("featureCount", 2);
        }

        @Test
        @DisplayName("merging degrades the result when either side is degraded")
        void mergeInheritsDegradation() {
            GeoJsonPayload healthy = GeoJsonPayload.of(List.of(),
                    Meta.live("USGS").with("featureCount", 0));
            GeoJsonPayload broken = GeoJsonPayload.of(List.of(),
                    Meta.live("NASA EONET").withDegraded("load balancer served RSS"));

            assertThat(Payloads.merge("tectonics", healthy, broken).meta().degraded())
                    .as("a half-broken composite must not look whole")
                    .isTrue();
        }

        @Test
        @DisplayName("mergeAll restamps the count across every contributor")
        void mergeAllRestampsCount() {
            GeoJsonPayload one = GeoJsonPayload.of(
                    List.of(GeoJson.feature("a", 1, 1, GeoJson.props("type", "a"))),
                    Meta.live("USGS").with("featureCount", 1));
            GeoJsonPayload two = GeoJsonPayload.of(
                    List.of(GeoJson.feature("b", 2, 2, GeoJson.props("type", "b"))),
                    Meta.live("NASA EONET").with("featureCount", 1));
            GeoJsonPayload three = GeoJsonPayload.of(List.of(),
                    Meta.live("FIRMS").with("featureCount", 0));

            GeoJsonPayload merged = Payloads.mergeAll("biosphere", one, two, three);

            assertThat(merged.features()).hasSize(2);
            assertThat(merged.meta().details()).containsEntry("featureCount", 2);
            assertThat(merged.meta().sources()).hasSize(3);
        }
    }

    @Test
    @DisplayName("a GeoJSON feature carries the fields Cesium needs")
    void featureShape() {
        JsonNode feature = GeoJson.feature("eq-1", 10.5, 20.5,
                GeoJson.props("type", "earthquake", "magnitude", 4.2));

        assertThat(feature.path("type").asText()).isEqualTo("Feature");
        assertThat(feature.path("id").asText()).isEqualTo("eq-1");
        assertThat(feature.path("geometry").path("type").asText()).isEqualTo("Point");
        assertThat(GeoJson.position(feature)).containsExactly(10.5, 20.5);
        assertThat(feature.path("properties").path("magnitude").asDouble()).isEqualTo(4.2);
        assertThat(Map.of("type", "earthquake")).containsEntry("type", "earthquake");
    }
}
