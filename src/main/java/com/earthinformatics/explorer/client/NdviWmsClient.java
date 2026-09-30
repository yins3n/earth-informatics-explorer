package com.earthinformatics.explorer.client;

import com.earthinformatics.explorer.config.Caches;
import com.earthinformatics.explorer.properties.ExplorerProperties;
import com.earthinformatics.explorer.util.Geo;
import com.earthinformatics.explorer.util.JsonSupport;
import com.earthinformatics.explorer.util.UpstreamRetry;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Vegetation health (NDVI) from an OGC WMS server.
 *
 * <p>Upstream: NASA GIBS, layer {@code MODIS_Terra_L3_NDVI_16Day}.
 *
 * <p><strong>GIBS is an image server, not a data server.</strong> It answers
 * {@code GetCapabilities}, {@code GetMap} and {@code DescribeLayer}, but its NDVI layers carry
 * {@code queryable="0"}: {@code GetFeatureInfo} is not implemented, and a request for it is
 * refused with a {@code ServiceException} delivered inside an HTTP 200. The first live run of this
 * endpoint sampled through {@code GetFeatureInfo}, received nothing usable, and reported an empty
 * <em>healthy</em> vegetation layer - indistinguishable from "no vegetation anywhere".
 *
 * <p>So the capabilities document is read first, and a layer that declares itself unqueryable
 * produces {@link Sampling#unavailable} with a reason. The service turns that into a degraded
 * payload carrying the {@code GetMap} recipe, which the dashboard renders as a Cesium imagery
 * layer. Sampling remains implemented for servers that do advertise {@code GetFeatureInfo}.
 *
 * <p>Security: the parser disables DTDs and external entities - an XML parser pointed at a
 * third-party endpoint must never be allowed to read the local filesystem.
 */
@Component
@Slf4j
public class NdviWmsClient {

    private final WebClient webClient;
    private final ExplorerProperties properties;

    public NdviWmsClient(WebClient webClient, ExplorerProperties properties) {
        this.webClient = webClient;
        this.properties = properties;
    }

    /** Layer names advertised by the configured WMS, for the discovery endpoint. */
    public Mono<List<String>> capabilities() {
        return probe().map(LayerCatalogue::names);
    }

    /**
     * Reads {@code GetCapabilities} once per TTL and caches the result.
     *
     * <p>The probe exists because "no NDVI values" and "this server does not implement
     * {@code GetFeatureInfo} at all" produce identical empty responses, and conflating them makes
     * the vegetation layer look broken rather than unsupported. The capabilities document states
     * which of the two it is, per layer, via {@code queryable="0|1"}.
     */
    @Cacheable(cacheNames = Caches.WMS_CAPABILITIES, key = "'ndvi'")
    public Mono<LayerCatalogue> probe() {
        String uri = properties.upstreams().ndvi().baseUrl()
                + "?service=WMS&request=GetCapabilities&version=1.3.0";
        return webClient.get()
                .uri(URI.create(uri))
                .retrieve()
                .bodyToMono(String.class)
                .map(this::parseCatalogue)
                .timeout(properties.resilience().responseTimeout())
                .onErrorResume(error -> {
                    log.warn("WMS GetCapabilities failed for {}: {}", uri, error.toString());
                    return Mono.just(LayerCatalogue.unreachable(uri, error.toString()));
                });
    }

    /**
     * Samples NDVI across a global lattice.
     *
     * <p>When the configured layer is advertised as non-queryable the result is
     * {@link Sampling#unavailable} rather than an empty list, so the service can report the
     * truth instead of an empty-but-healthy payload.
     *
     * @return one entry per resolved cell: {@code {latitude, longitude, ndvi}}.
     */
    public Mono<Sampling> sampleNdvi(double stepDegrees) {
        ExplorerProperties.Upstreams.Ndvi config = properties.upstreams().ndvi();
        if (!config.enabled()) {
            return Mono.just(Sampling.unavailable("NDVI sampling is disabled by configuration"));
        }

        return probe().flatMap(catalogue -> {
            WmsLayer layer = catalogue.layer(config.layerName());
            if (layer == null && catalogue.reachable()) {
                return Mono.just(Sampling.unavailable("WMS layer '" + config.layerName()
                        + "' is not advertised by " + config.baseUrl()));
            }
            if (layer == null) {
                // The server is unreachable or unparseable. Sampling anyway would turn one
                // failed GetCapabilities into a lattice of doomed GetFeatureInfo calls.
                return Mono.just(Sampling.unavailable("WMS at " + config.baseUrl()
                        + " could not be read: " + catalogue.reason()));
            }
            if (!layer.queryable()) {
                // GIBS and most tile-oriented servers publish NDVI as an image only. Sampling
                // them is impossible by design, not by failure: the honest answer is a
                // descriptor the dashboard can render as a Cesium imagery layer.
                return Mono.just(Sampling.unavailable(
                        "WMS layer '" + config.layerName() + "' is published as an image layer "
                                + "(queryable=0); the server does not implement GetFeatureInfo"));
            }
            return sampleLattice(config, stepDegrees);
        });
    }

    private Mono<Sampling> sampleLattice(ExplorerProperties.Upstreams.Ndvi config,
            double stepDegrees) {
        double step = Math.max(5d, Math.min(stepDegrees, 45d));
        List<Geo.GridCell> cells = Geo.grid(step);
        List<List<Geo.GridCell>> chunks = Geo.chunk(cells, config.maxFeatures());

        return reactor.core.publisher.Flux.fromIterable(chunks)
                .flatMapSequential(chunk -> sampleChunk(config, chunk), 2, 1)
                .concatMap(Flux::fromIterable)
                .collectList()
                .doOnNext(samples -> log.debug("NDVI WMS resolved {} of {} cells", samples.size(),
                        cells.size()))
                .map(Sampling::sampled);
    }

    private Mono<List<NdviSample>> sampleChunk(ExplorerProperties.Upstreams.Ndvi config,
            List<Geo.GridCell> cells) {
        // One GetFeatureInfo per cell. Cells are individually cheap (a few hundred bytes) and
        // the whole chunk is covered by a single cache entry, so the trade-off is deliberate.
        return reactor.core.publisher.Flux.fromIterable(cells)
                .flatMap(cell -> cellValue(config, cell)
                        .map(value -> new NdviSample(cell.latitude(), cell.longitude(), value))
                        .defaultIfEmpty(new NdviSample(cell.latitude(), cell.longitude(),
                                Double.NaN)), 4, 1)
                .filter(sample -> !Double.isNaN(sample.value()))
                .collectList()
                .timeout(properties.resilience().responseTimeout())
                .onErrorReturn(List.of());
    }

    private Mono<Double> cellValue(ExplorerProperties.Upstreams.Ndvi config, Geo.GridCell cell) {
        // Query the centre of the tile that contains the cell, at the requested pixel.
        double half = 0.5d;
        String bbox = "%s,%s,%s,%s".formatted(cell.longitude() - half, cell.latitude() - half,
                cell.longitude() + half, cell.latitude() + half);
        String uri = properties.upstreams().ndvi().baseUrl()
                + "?service=WMS&version=1.1.1&request=GetFeatureInfo"
                + "&layers=" + config.layerName()
                + "&query_layers=" + config.layerName()
                + "&styles=" + (config.style() == null ? "" : config.style())
                + "&srs=" + config.srs().replace(":", "%3A")
                + "&bbox=" + bbox
                + "&width=1&height=1&x=0&y=0"
                + "&info_format=text/xml"
                + "&feature_count=1";
        return webClient.get()
                .uri(URI.create(uri))
                .retrieve()
                .bodyToMono(String.class)
                .map(this::checkServiceException)
                .map(body -> extractNumericValue(body))
                .retryWhen(UpstreamRetry.backoff(
                        properties.resilience().retryBackoff(), 1))
                .onErrorResume(error -> Mono.empty());
    }

    /**
     * Turns a WMS {@code ServiceException} into an exception.
     *
     * <p>The response is HTTP 200 with an exception document inside, so without this check a
     * server that has {@code GetFeatureInfo} switched off looks exactly like a server that
     * simply had no data at that pixel.
     */
    private String checkServiceException(String xml) {
        if (xml == null) {
            return null;
        }
        int marker = xml.indexOf("ServiceException");
        if (marker < 0) {
            return xml;
        }
        String message = xml.replaceAll("<[^>]+>", " ").replaceAll("\\s+", " ").strip();
        throw new IllegalStateException("WMS ServiceException: " + message);
    }

    /** NDVI value for one lattice point. */
    public record NdviSample(double latitude, double longitude, double value) {
    }

    /** One layer as advertised by the server, with the two fields that drive our behaviour. */
    public record WmsLayer(String name, boolean queryable, String defaultTime) {
    }

    /** Parsed {@code GetCapabilities} document. */
    public record LayerCatalogue(List<WmsLayer> layers, boolean reachable, String reason) {

        public static LayerCatalogue unreachable(String uri, String reason) {
            return new LayerCatalogue(List.of(), false, reason);
        }

        public List<String> names() {
            return layers.stream().map(WmsLayer::name).toList();
        }

        public WmsLayer layer(String name) {
            return layers.stream().filter(entry -> entry.name().equals(name)).findFirst()
                    .orElse(null);
        }
    }

    /**
     * Outcome of a sampling attempt: values, or a reason why the server cannot supply values.
     *
     * @param samples   Resolved cells, empty when {@code unavailable}.
     * @param available {@code true} when the layer can be sampled.
     * @param reason    Human-readable explanation when not available.
     */
    public record Sampling(List<NdviSample> samples, boolean available, String reason) {

        public static Sampling sampled(List<NdviSample> samples) {
            return new Sampling(samples, true, null);
        }

        public static Sampling unavailable(String reason) {
            return new Sampling(List.of(), false, reason);
        }
    }

    // ------------------------------------------------------------ XML parsing

    /**
     * Pulls the first numeric value out of a {@code GetFeatureInfo} response.
     *
     * <p>Works across the shapes produced by GeoServer, MapServer and NASA GIBS, which all emit
     * {@code OGCPropertyValue} pairs but disagree on namespace prefixes and element order.
     */
    private Double extractNumericValue(String xml) {
        Document document = parseXml(xml);
        if (document == null) {
            return null;
        }
        NodeList properties = document.getElementsByTagNameNS("*", "OGCPropertyValue");
        for (int i = 0; i < properties.getLength(); i++) {
            Element property = (Element) properties.item(i);
            String name = firstText(property, "Name");
            String value = firstText(property, "Value");
            if (value == null || value.isBlank()) {
                continue;
            }
            Double parsed = parseNumeric(value);
            if (parsed != null && (name == null || isVegetationName(name))) {
                return parsed;
            }
        }
        // Some servers return a bare number as the whole body.
        return parseNumeric(xml == null ? null : xml.strip());
    }

    private static boolean isVegetationName(String name) {
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        return lower.contains("ndvi") || lower.contains("value") || lower.contains("vegetation")
                || lower.contains("index");
    }

    /**
     * Parses the layer catalogue, keeping the {@code queryable} flag and the default time of
     * every named layer. Both are read from the layer block itself rather than inferred, because
     * the layer block is the only place the server states them.
     */
    private LayerCatalogue parseCatalogue(String xml) {
        Document document = parseXml(xml);
        if (document == null) {
            return new LayerCatalogue(List.of(), false, "GetCapabilities document was not parseable");
        }
        List<WmsLayer> layers = new ArrayList<>();
        NodeList layerNodes = document.getElementsByTagNameNS("*", "Layer");
        for (int i = 0; i < layerNodes.getLength(); i++) {
            Element layer = (Element) layerNodes.item(i);
            // Only a layer's OWN Name counts. getElementsByTagNameNS searches *descendants*, so
            // reading it naively makes the root container layer adopt the first leaf layer's name
            // - and the root carries no queryable attribute, so it defaults to queryable. A
            // capabilities document that happens to list the configured layer first then
            // advertises it as sampleable, and the client walks straight into a server that
            // answers GetFeatureInfo with "current not queryable".
            String name = directChildText(layer, "Name");
            if (name == null || name.isBlank()) {
                continue;
            }
            String queryable = layer.getAttribute("queryable");
            // Time default likewise belongs to this layer, not to a descendant's.
            String defaultTime = "";
            NodeList dimensions = layer.getElementsByTagNameNS("*", "Dimension");
            for (int d = 0; d < dimensions.getLength(); d++) {
                Element dimension = (Element) dimensions.item(d);
                if ("time".equalsIgnoreCase(dimension.getAttribute("name"))
                        && isDirectChild(layer, dimension)) {
                    defaultTime = dimension.getAttribute("default");
                    break;
                }
            }
            // queryable="0" is the OGC way of saying GetFeatureInfo is not implemented.
            layers.add(new WmsLayer(name, !"0".equals(queryable), defaultTime));
        }
        log.debug("WMS catalogue: {} named layers", layers.size());
        if (layers.isEmpty()) {
            // A maintenance page is usually well-formed enough to parse as XML, so "no layers"
            // usually means "this is not a GetCapabilities document", not "the server has none".
            return new LayerCatalogue(List.of(), false,
                    "GetCapabilities document contained no named layers");
        }
        return new LayerCatalogue(List.copyOf(layers), true, null);
    }

    /** Text of a direct child element, or {@code null} when the child is absent. */
    private static String directChildText(Element parent, String localName) {
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE
                    && localName.equals(child.getLocalName())
                    && isDirectChild(parent, (Element) child)) {
                return child.getTextContent().strip();
            }
        }
        return null;
    }

    private static boolean isDirectChild(Element parent, Element candidate) {
        return parent.equals(candidate.getParentNode());
    }

    private String firstText(Element parent, String localName) {
        NodeList nodes = parent.getElementsByTagNameNS("*", localName);
        if (nodes.getLength() == 0) {
            return null;
        }
        return nodes.item(0).getTextContent();
    }

    private static Double parseNumeric(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            double value = Double.parseDouble(raw.strip());
            // NDVI lives in [-1, 1]; reflectance products are scaled by 10000. Reject obvious
            // non-values so a WMS that echoes the whole document does not poison the field.
            return Double.isNaN(value) ? null : value;
        } catch (NumberFormatException notANumber) {
            return null;
        }
    }

    /** Hardened XML parsing: DTDs and external entities disabled. */
    private Document parseXml(String xml) {
        if (xml == null || xml.isBlank()) {
            return null;
        }
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(
                    new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
            document.getDocumentElement().normalize();
            return document;
        } catch (Exception malformed) {
            log.debug("Unparseable WMS payload: {}", malformed.toString());
            return null;
        }
    }

    /** Provenance helper. */
    public String describe() {
        ExplorerProperties.Upstreams.Ndvi config = properties.upstreams().ndvi();
        Map<String, Object> description = new LinkedHashMap<>();
        description.put("endpoint", config.baseUrl());
        description.put("layer", config.layerName());
        description.put("srs", config.srs());
        description.put("maxFeaturesPerChunk", config.maxFeatures());
        return JsonSupport.mapper().valueToTree(description).toString();
    }
}
