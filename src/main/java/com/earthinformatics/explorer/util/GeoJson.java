package com.earthinformatics.explorer.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.Collection;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.earthinformatics.explorer.dto.GeoJsonPayload;
import com.earthinformatics.explorer.dto.Meta;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GeoJSON construction helpers.
 *
 * <p>Two flavours are supported deliberately:
 * <ul>
 *   <li><b>Pass-through</b> - {@link #augment(JsonNode, String, Map)} keeps an upstream feature
 *       byte-for-byte and only injects/normalises the handful of properties the renderer needs.
 *       USGS and EONET documents are large and well specified; re-shaping them wholesale would
 *       add CPU cost and risk losing data.</li>
 *   <li><b>Synthesis</b> - {@link #feature(String, JsonNode, Map)} builds features from scratch
 *       for providers that return bespoke JSON (Open-Meteo, OpenSky, NOAA CO-OPS).</li>
 * </ul>
 */
public final class GeoJson {

    private GeoJson() {
    }

    /** Ordered key/value map builder: {@code props("mag", 4.2, "place", "off coast")}. */
    public static Map<String, Object> props(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("props() requires an even number of arguments");
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return map;
    }

    /** Point geometry in RFC 7946 axis order: {@code [longitude, latitude]}. */
    public static ObjectNode point(double longitude, double latitude) {
        ObjectNode geometry = JsonSupport.nodes().objectNode();
        geometry.put("type", "Point");
        ArrayNode coordinates = geometry.putArray("coordinates");
        coordinates.add(round6(longitude));
        coordinates.add(round6(latitude));
        return geometry;
    }

    /** LineString geometry from an ordered {@code [lon, lat]} array. */
    public static ObjectNode line(double[][] lonLatPairs) {
        ObjectNode geometry = JsonSupport.nodes().objectNode();
        geometry.put("type", "LineString");
        ArrayNode coordinates = geometry.putArray("coordinates");
        for (double[] pair : lonLatPairs) {
            coordinates.add(round6(pair[0]));
            coordinates.add(round6(pair[1]));
        }
        return geometry;
    }

    /**
     * Area geometry (Polygon / MultiPolygon) from an upstream {@code coordinates} array.
     *
     * <p>Only {@code type} and {@code coordinates} are kept. Providers such as EONET store
     * {@code magnitudeValue}, {@code date} and friends as siblings of the GeoJSON members, and
     * copying the whole node would emit non-standard members inside {@code geometry}, which
     * strict clients reject. The coordinates are deep-copied so the cached payload never shares
     * mutable state with the parsed upstream document.
     */
    public static ObjectNode geometry(String type, JsonNode coordinates) {
        ObjectNode geometry = JsonSupport.nodes().objectNode();
        geometry.put("type", type);
        geometry.set("coordinates", coordinates.deepCopy());
        return geometry;
    }

    /** Complete Feature node with a stable {@code id} (critical for de-duplicating on refresh). */
    public static ObjectNode feature(String id, JsonNode geometry, Map<String, Object> properties) {
        ObjectNode node = JsonSupport.nodes().objectNode();
        node.put("type", "Feature");
        if (id != null) {
            node.put("id", id);
        }
        node.set("geometry", geometry);
        node.set("properties", propertiesNode(properties));
        return node;
    }

    public static ObjectNode feature(String id, double longitude, double latitude,
            Map<String, Object> properties) {
        return feature(id, point(longitude, latitude), properties);
    }

    /**
     * Pass-through overload: attaches an <em>existing</em> properties node rather than a map,
     * which keeps upstream property trees intact instead of flattening them to strings.
     */
    public static ObjectNode feature(String id, JsonNode geometry, JsonNode properties) {
        ObjectNode node = JsonSupport.objectNode();
        node.put("type", "Feature");
        if (id != null) {
            node.put("id", id);
        }
        node.set("geometry", geometry);
        node.set("properties", properties != null ? properties : JsonSupport.objectNode());
        return node;
    }

    public static ObjectNode propertiesNode(Map<String, Object> properties) {
        ObjectNode node = JsonSupport.nodes().objectNode();
        properties.forEach((key, value) -> node.set(key, valueNode(value)));
        return node;
    }

    /**
     * Converts an arbitrary property value to JSON.
     *
     * <p>Collections are handled explicitly. Without that branch a {@code List} falls through to
     * {@code String.valueOf} and reaches the payload as {@code "[Volcanoes]"} - a Java toString,
     * not JSON. A client then has to re-parse a string to recover an array, and the value no
     * longer round-trips.
     */
    private static JsonNode valueNode(Object value) {
        if (value == null) {
            return JsonSupport.nodes().nullNode();
        }
        if (value instanceof JsonNode node) {
            return node;
        }
        if (value instanceof RawJson raw) {
            return raw.node();
        }
        if (value instanceof Number number) {
            double d = number.doubleValue();
            // An if/else, not a ternary: numeric promotion would make both ternary branches
            // double and route every value through numberNode(double), so integral properties
            // (ids, timestamps, counts) would serialise as 1781481600000.0 instead of 1781481600000.
            if (d == Math.rint(d) && Math.abs(d) < 1e15 && Double.isFinite(d)) {
                return JsonSupport.nodes().numberNode(number.longValue());
            }
            return JsonSupport.nodes().numberNode(d);
        }
        if (value instanceof Boolean bool) {
            return JsonSupport.nodes().booleanNode(bool);
        }
        if (value instanceof Map<?, ?> map) {
            return propertiesNode(castMap(map));
        }
        if (value instanceof Collection<?> collection) {
            ArrayNode array = JsonSupport.nodes().arrayNode();
            collection.forEach(element -> array.add(valueNode(element)));
            return array;
        }
        return JsonSupport.nodes().textNode(String.valueOf(value));
    }

    /** Escape hatch for embedding an already-built JSON tree as a property value. */
    public record RawJson(JsonNode node) {
    }

    /**
     * Pass-through augmentation: keeps an upstream Feature intact while guaranteeing the
     * renderer-facing properties exist and are consistently named.
     *
     * @param upstream  Feature node from the provider (mutated copies are avoided; we mutate in
     *                  place because the node never leaves this layer).
     * @param fallbackId Identifier to use when the provider omits one.
     * @param extras    Normalised properties injected/overwritten by this service.
     */
    public static JsonNode augment(JsonNode upstream, String fallbackId,
            Map<String, Object> extras) {
        if (!(upstream instanceof ObjectNode feature)) {
            return upstream;
        }
        if (!feature.hasNonNull("type")) {
            feature.put("type", "Feature");
        }
        if (!feature.hasNonNull("geometry") || feature.get("geometry").isNull()) {
            return null;
        }
        if (!feature.hasNonNull("id")) {
            feature.put("id", fallbackId);
        }
        // withObject() cannot create a missing field - it throws "Cannot replace MissingNode"
        // - and several providers (EONET among them) publish Features with no properties member
        // at all, so the node is created explicitly when absent.
        JsonNode existing = feature.get("properties");
        ObjectNode properties = existing instanceof ObjectNode object
                ? object
                : feature.putObject("properties");
        extras.forEach((key, value) -> {
            if (value != null) {
                properties.set(key, propertiesNode(Map.of(key, value)).get(key));
            }
        });
        return feature;
    }

    /** Extracts {@code [lon, lat]} from a Point geometry, or {@code null} if not a point. */
    public static double[] position(JsonNode feature) {
        JsonNode geometry = feature.path("geometry");
        if (!"Point".equals(geometry.path("type").asText())) {
            return null;
        }
        JsonNode coordinates = geometry.path("coordinates");
        if (!coordinates.isArray() || coordinates.size() < 2) {
            return null;
        }
        return new double[]{coordinates.get(0).asDouble(), coordinates.get(1).asDouble()};
    }

    /** Reads a feature property, tolerating both JSON numbers and numeric strings. */
    public static double propertyAsDouble(JsonNode feature, String name, double fallback) {
        JsonNode value = feature.path("properties").path(name);
        if (value.isNumber()) {
            return value.asDouble();
        }
        if (value.isTextual()) {
            try {
                return Double.parseDouble(value.asText().trim());
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }

    public static long propertyAsLong(JsonNode feature, String name, long fallback) {
        JsonNode value = feature.path("properties").path(name);
        if (value.isNumber()) {
            return value.asLong();
        }
        if (value.isTextual()) {
            try {
                return Long.parseLong(value.asText().trim());
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }

    /** Reads a boolean feature property, tolerating stringified booleans. */
    public static boolean propertyAsBoolean(JsonNode feature, String name, boolean fallback) {
        JsonNode value = feature.path("properties").path(name);
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        if (value.isNumber()) {
            return value.asInt() != 0;
        }
        if (value.isTextual()) {
            return switch (value.asText().trim().toLowerCase(java.util.Locale.ROOT)) {
                case "true", "1", "yes" -> true;
                case "false", "0", "no" -> false;
                default -> fallback;
            };
        }
        return fallback;
    }

    public static String propertyAsString(JsonNode feature, String name, String fallback) {
        JsonNode value = feature.path("properties").path(name);
        return value.isMissingNode() || value.isNull() ? fallback : value.asText();
    }

    /**
     * Builds a square cell polygon around a lattice point - the primitive used by the sea
     * surface temperature and vegetation heatmaps, which render as a single translucent
     * {@code PerInstanceColorAppearance} primitive client-side.
     */
    public static ObjectNode cellPolygon(double longitude, double latitude, double halfSize) {
        ObjectNode geometry = JsonSupport.nodes().objectNode();
        geometry.put("type", "Polygon");
        ArrayNode rings = geometry.putArray("coordinates");
        ArrayNode ring = rings.addArray();
        double[][] corners = {
                {longitude - halfSize, latitude - halfSize},
                {longitude + halfSize, latitude - halfSize},
                {longitude + halfSize, latitude + halfSize},
                {longitude - halfSize, latitude + halfSize},
                {longitude - halfSize, latitude - halfSize}};
        for (double[] corner : corners) {
            ring.add(round6(Geo.wrapLongitude(corner[0])));
            ring.add(round6(Math.max(-90, Math.min(90, corner[1]))));
        }
        return geometry;
    }

    /** Orders a FeatureCollection so the renderer always sees a stable, repeatable layout. */
    public static List<JsonNode> sortedByTimestamp(List<JsonNode> features) {
        List<JsonNode> copy = new ArrayList<>(features);
        copy.sort((left, right) -> Long.compare(
                propertyAsLong(left, "eventTime", 0L),
                propertyAsLong(right, "eventTime", 0L)));
        return copy;
    }

    /** Convenience: wrap features plus meta into the standard response object. */
    public static GeoJsonPayload payload(List<JsonNode> features, Meta meta) {
        return GeoJsonPayload.of(features, meta);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Map<?, ?> source) {
        Map<String, Object> result = new LinkedHashMap<>();
        source.forEach((key, value) -> result.put(String.valueOf(key), value));
        return result;
    }

    /** Coordinate precision beyond ~11 cm is noise for a planetary globe and costs payload bytes. */
    private static double round6(double value) {
        return Math.round(value * 1_000_000d) / 1_000_000d;
    }
}
