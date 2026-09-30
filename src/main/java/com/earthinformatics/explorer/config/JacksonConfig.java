package com.earthinformatics.explorer.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * JSON conventions.
 *
 * <ul>
 *   <li>{@code non_null} keeps degraded payloads small - absent fields are simply omitted rather
 *       than serialised as {@code null}, which matters for multi-megabyte wildfire documents.</li>
 *   <li>ISO-8601 timestamps as strings, not epoch arrays, so the browser can parse them without
 *       a lookup table.</li>
 *   <li>Unknown upstream properties are tolerated: providers add fields constantly and a strict
 *       mapper would start throwing after the next upstream release.</li>
 * </ul>
 */
@Configuration
public class JacksonConfig {

    @Bean
    public Jackson2ObjectMapperBuilderCustomizer jsonCustomizer() {
        return builder -> builder
                .modules(new JavaTimeModule())
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
                        SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .featuresToEnable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    }
}
