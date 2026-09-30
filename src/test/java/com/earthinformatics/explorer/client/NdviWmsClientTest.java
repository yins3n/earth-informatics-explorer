package com.earthinformatics.explorer.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.earthinformatics.explorer.TestProperties;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.test.StepVerifier;

/**
 * NASA GIBS WMS behaviour.
 *
 * <p>NASA GIBS is a raster-only service. It will happily answer {@code GetCapabilities}, a
 * {@code GetMap} and a {@code DescribeLayer} - and its NDVI layers carry {@code queryable="0"},
 * which is the OGC way of saying {@code GetFeatureInfo} is not implemented for them. It is an
 * image server that is very good at not being a data server.
 *
 * <p>The first live run of the vegetation endpoint tried to sample NDVI through
 * {@code GetFeatureInfo}, got nothing back, and returned an empty, <em>non-degraded</em> layer:
 * indistinguishable, to the dashboard and to a user, from "no vegetation on Earth". These tests
 * exist so the honest answer stays the answer, and so that reintroducing vector sampling as a
 * "fix" fails here rather than shipping a silently blank layer again.
 */
class NdviWmsClientTest {

    /**
     * A deliberately GIBS-shaped capabilities document: the container layer is queryable, the
     * NDVI leaf is not, and the time dimension carries a default the renderer can request.
     */
    private static final String CAPABILITIES = """
            <?xml version="1.0" encoding="UTF-8"?>
            <WMS_Capabilities version="1.3.0" xmlns="http://www.opengis.net/wms"
                              xmlns:xlink="http://www.w3.org/1999/xlink">
              <Service><Name>WMS</Name><Title>NASA GIBS</Title></Service>
              <Capability>
                <Request>
                  <GetCapabilities><Format>text/xml</Format></GetCapabilities>
                  <GetMap><Format>image/png</Format></GetMap>
                  <GetFeatureInfo><Format>text/html</Format></GetFeatureInfo>
                </Request>
                <Exception><Format>XML</Format></Exception>
                <Layer>
                  <Title>Root</Title>
                  <CRS>EPSG:4326</CRS>
                  <Layer queryable="0" opaque="0" cascaded="0">
                    <Name>MODIS_Terra_L3_NDVI_16Day</Name>
                    <Title>MODIS_Terra_L3_NDVI_16Day</Title>
                    <CRS>EPSG:4326</CRS>
                    <CRS>EPSG:3857</CRS>
                    <EX_GeographicBoundingBox>
                      <westBoundLongitude>-180.000000</westBoundLongitude>
                      <eastBoundLongitude>180.000000</eastBoundLongitude>
                      <southBoundLatitude>-90.000000</southBoundLatitude>
                      <northBoundLatitude>90.000000</northBoundLatitude>
                    </EX_GeographicBoundingBox>
                    <BoundingBox CRS="EPSG:4326" minx="-90.0" miny="-180.0"
                                  maxx="90.0" maxy="180.0" />
                    <Dimension name="time" units="ISO8601" default="2026-08-29" nearestValue="0">\
            2000-03-05/2000-12-18/P16D,2026-01-01/2026-08-29/P16D</Dimension>
                    <Style><Name>default</Name><Title>default</Title></Style>
                  </Layer>
                  <Layer queryable="0" opaque="0" cascaded="0">
                    <Name>MODIS_Terra_Aerosol</Name>
                    <Title>MODIS_Terra_Aerosol</Title>
                  </Layer>
                </Layer>
              </Capability>
            </WMS_Capabilities>
            """;

    private MockWebServer server;
    private NdviWmsClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        client = new NdviWmsClient(WebClient.builder().build(),
                TestProperties.builder()
                        .ndvi(server.url("/wms.cgi").toString(), "MODIS_Terra_L3_NDVI_16Day")
                        .fast()
                        .build());
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    private void enqueueCapabilities() {
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", MediaType.TEXT_XML_VALUE)
                .setBody(CAPABILITIES));
    }

    private void enqueueCapabilitiesFailure() {
        for (int attempt = 0; attempt < 2; attempt++) {
            server.enqueue(new MockResponse()
                    .setResponseCode(HttpStatus.SERVICE_UNAVAILABLE.value()));
        }
    }

    @Test
    @DisplayName("reads queryability from the layer block, not from a guess")
    void detectsUnqueryableLayer() {
        enqueueCapabilities();

        StepVerifier.create(client.probe())
                .assertNext(catalogue -> {
                    assertThat(catalogue.reachable()).isTrue();
                    assertThat(catalogue.names()).contains("MODIS_Terra_L3_NDVI_16Day");
                    assertThat(catalogue.layer("MODIS_Terra_L3_NDVI_16Day").queryable())
                            .as("GIBS NDVI is published as an image layer")
                            .isFalse();
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("captures the time default the renderer needs to build a valid GetMap")
    void capturesDefaultTime() {
        enqueueCapabilities();

        StepVerifier.create(client.probe())
                .assertNext(catalogue -> assertThat(
                        catalogue.layer("MODIS_Terra_L3_NDVI_16Day").defaultTime())
                        .isEqualTo("2026-08-29"))
                .verifyComplete();
    }

    @Test
    @DisplayName("reports sampling as unavailable, with a reason, not as an empty result")
    void samplingIsUnavailable() {
        enqueueCapabilities();

        StepVerifier.create(client.sampleNdvi(10))
                .assertNext(sampling -> {
                    assertThat(sampling.available()).as("GetFeatureInfo availability").isFalse();
                    assertThat(sampling.samples()).isEmpty();
                    assertThat(sampling.reason())
                            .containsIgnoringCase("queryable")
                            .contains("MODIS_Terra_L3_NDVI_16Day");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("makes no GetFeatureInfo calls once the layer is known to be unqueryable")
    void doesNotWasteRequestsOnAnUnqueryableLayer() throws InterruptedException {
        enqueueCapabilities();

        StepVerifier.create(client.sampleNdvi(30)).expectNextCount(1).verifyComplete();

        // Only the capabilities probe was made. A sampling attempt here would be the bug this
        // endpoint had on its first live run: hundreds of 200-with-an-exception responses.
        assertThat(server.getRequestCount()).isEqualTo(1);
        assertThat(server.takeRequest().getPath()).contains("request=GetCapabilities");
    }

    @Test
    @DisplayName("says the layer is missing when the server is up but does not serve it")
    void distinguishesMissingLayerFromUnqueryable() {
        enqueueCapabilities();

        NdviWmsClient wrongLayer = new NdviWmsClient(WebClient.builder().build(),
                TestProperties.builder()
                        .ndvi(server.url("/wms.cgi").toString(), "NO_SUCH_LAYER")
                        .fast()
                        .build());

        StepVerifier.create(wrongLayer.sampleNdvi(10))
                .assertNext(sampling -> {
                    assertThat(sampling.available()).isFalse();
                    assertThat(sampling.reason())
                            .contains("NO_SUCH_LAYER")
                            .containsIgnoringCase("not advertised");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("degrades with a reason when capabilities are unreachable")
    void degradesWhenCapabilitiesFail() {
        enqueueCapabilitiesFailure();

        StepVerifier.create(client.probe())
                .assertNext(catalogue -> {
                    assertThat(catalogue.reachable()).isFalse();
                    assertThat(catalogue.reason()).isNotBlank();
                    assertThat(catalogue.layers()).isEmpty();
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("an unreachable server also reports sampling as unavailable")
    void unreachableServerIsNotAvailable() {
        enqueueCapabilitiesFailure();

        StepVerifier.create(client.sampleNdvi(10))
                .assertNext(sampling -> assertThat(sampling.available()).isFalse())
                .verifyComplete();
    }

    @Test
    @DisplayName("a maintenance page is not mistaken for an empty catalogue")
    void unparseableCapabilitiesDegrade() {
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", MediaType.TEXT_HTML_VALUE)
                .setBody("<html><body>we are down for maintenance</body></html>"));

        StepVerifier.create(client.probe())
                .assertNext(catalogue -> {
                    assertThat(catalogue.reachable()).isFalse();
                    assertThat(catalogue.reason()).containsIgnoringCase("no named layers");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("refuses to let an XXE payload reach the filesystem")
    void resistsExternalEntities() {
        // The parser is pointed at a third-party endpoint; a DTD-capable parser would let that
        // endpoint read local files and hand them back as layer names.
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", MediaType.TEXT_XML_VALUE)
                .setBody("""
                        <?xml version="1.0"?>
                        <!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
                        <WMS_Capabilities version="1.3.0">
                          <Layer><Name>&xxe;</Name><queryable>0</queryable></Layer>
                        </WMS_Capabilities>
                        """));

        StepVerifier.create(client.probe())
                .assertNext(catalogue -> assertThat(catalogue.names())
                        .as("no file contents leaked into the catalogue")
                        .noneMatch(name -> name.contains("root:")))
                .verifyComplete();
    }

    @Test
    @DisplayName("exposes the provenance a degraded payload needs to render the image")
    void describesTheWmsTarget() {
        String description = client.describe();

        assertThat(description)
                .contains("MODIS_Terra_L3_NDVI_16Day")
                .contains("EPSG:4326");
        assertThat(description).contains("/wms.cgi");
    }

    @Test
    @DisplayName("treats a WMS ServiceException as a failure, not as data")
    void serviceExceptionIsAnError() {
        // A WMS that answers HTTP 200 with a ServiceExceptionReport is signalling an error. A
        // client that concatenates the body into a value would parse "<ServiceException ...>text"
        // as an NDVI reading, and the failure would be invisible in the rendered layer.
        String xml = """
                <ServiceExceptionReport version="1.3.0">
                  <ServiceException code="LayerNotQueryable">current not queryable</ServiceException>
                </ServiceExceptionReport>
                """;

        NdviWmsClient queryable = new NdviWmsClient(WebClient.builder().build(),
                TestProperties.builder()
                        .ndvi(server.url("/wms.cgi").toString(), "MODIS_Terra_L3_NDVI_16Day")
                        .fast()
                        .build());
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", MediaType.TEXT_XML_VALUE)
                .setBody(xml));
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", MediaType.TEXT_XML_VALUE)
                .setBody(xml));
        // Capabilities that advertise the layer as queryable, so sampling is actually attempted.
        server.enqueue(new MockResponse()
                .setResponseCode(HttpStatus.OK.value())
                .setHeader("Content-Type", MediaType.TEXT_XML_VALUE)
                .setBody(CAPABILITIES.replaceFirst("<Layer queryable=\"0\"",
                        "<Layer queryable=\"1\"")));

        StepVerifier.create(queryable.sampleNdvi(45))
                .assertNext(sampling -> {
                    // Either the ServiceException is swallowed per cell, or it fails the chunk -
                    // both are acceptable, but no value may be invented from the error document.
                    assertThat(sampling.samples())
                            .as("no NDVI value parsed out of an error document")
                            .allSatisfy(sample ->
                                    assertThat(sample.value())
                                            .as("value at %s,%s", sample.latitude(),
                                                    sample.longitude())
                                            .isNotNaN());
                })
                .verifyComplete();
    }

}
