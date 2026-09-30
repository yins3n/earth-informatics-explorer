package com.earthinformatics.explorer.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Single source of truth for imperative JSON tree building.
 *
 * <p>Jackson's {@code JsonNodeFactory} is immutable and thread-safe, so a shared static
 * instance is safe and avoids allocating a factory per feature - which matters when a
 * wildfire response contains tens of thousands of nodes.
 */
public final class JsonSupport {

    private static final JsonNodeFactory NODES = JsonNodeFactory.withExactBigDecimals(false);
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private JsonSupport() {
    }

    public static JsonNodeFactory nodes() {
        return NODES;
    }

    public static ObjectNode objectNode() {
        return NODES.objectNode();
    }

    public static ArrayNode arrayNode() {
        return NODES.arrayNode();
    }

    /** Read-only mapper for local structural queries; never used for HTTP message bodies. */
    public static ObjectMapper mapper() {
        return MAPPER;
    }

    /**
     * Best-effort parse used when a provider occasionally returns an empty body or an HTML
     * error page: never throws, returns {@code null} so callers can degrade gracefully.
     */
    public static JsonNode parseOrNull(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return MAPPER.readTree(raw);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
