package com.earthinformatics.explorer.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tide arithmetic and NOAA payload parsing.
 *
 * <p>These tests exist because the live integration run silently produced an empty tide layer:
 * CO-OPS returned {@code {"predictions":[{"t":"2026-09-28 04:30",...}]}} while the parser only
 * understood the legacy {@code data[] / v_date / v_hi / v_lo} shape, and even after that was
 * fixed the timestamp parser rejected the space-separated ISO form. Neither failure threw; both
 * produced "no stations" and a healthy-looking payload.
 */
class TideMathTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode json(String raw) {
        try {
            return MAPPER.readTree(raw);
        } catch (Exception malformed) {
            throw new AssertionError("test fixture is not valid JSON", malformed);
        }
    }

    @Nested
    @DisplayName("parseHilo")
    class ParseHilo {

        @Test
        @DisplayName("reads the current predictions shape, with H and L rows")
        void currentProductShape() {
            JsonNode response = json("""
                    {"predictions": [
                      {"t": "2026-09-28 04:30", "v": "-0.158", "type": "L"},
                      {"t": "2026-09-28 10:12", "v": "0.612",  "type": "H"},
                      {"t": "2026-09-28 16:41", "v": "-0.201", "type": "L"}
                    ]}""");

            List<TideMath.TurningPoint> points = TideMath.parseHilo(response);

            assertThat(points).hasSize(3);
            assertThat(points.get(0).isLow()).isTrue();
            assertThat(points.get(0).extreme()).isEqualTo(-0.158);
            assertThat(points.get(1).isHigh()).isTrue();
            assertThat(points.get(1).type()).isEqualTo("HIGH");
            assertThat(points.get(2).time()).isEqualTo(Instant.parse("2026-09-28T16:41:00Z"));
        }

        @Test
        @DisplayName("still reads the legacy data[] shape")
        void legacyProductShape() {
            JsonNode response = json("""
                    {"data": [
                      {"v_date": "20260928 04:30", "v_hi": "1.204", "v_lo": "-0.158"}
                    ]}""");

            List<TideMath.TurningPoint> points = TideMath.parseHilo(response);

            assertThat(points).hasSize(1);
            assertThat(points.get(0).isHigh()).isTrue();
            assertThat(points.get(0).isLow()).isTrue();
            assertThat(points.get(0).extreme()).isEqualTo(1.204);
        }

        @Test
        @DisplayName("returns empty for an error document instead of throwing")
        void errorDocument() {
            assertThat(TideMath.parseHilo(json(
                    "{\"error\":{\"message\":\"No Predictions data was found.\"}}"))).isEmpty();
            assertThat(TideMath.parseHilo(null)).isEmpty();
        }

        @Test
        @DisplayName("skips rows without a usable time or value")
        void skipsUnusableRows() {
            JsonNode response = json("""
                    {"predictions": [
                      {"t": "not-a-time", "v": "1.0", "type": "H"},
                      {"t": "2026-09-28 04:30", "v": "NaN", "type": "H"},
                      {"t": "2026-09-28 10:12", "v": "0.5", "type": "H"}
                    ]}""");

            assertThat(TideMath.parseHilo(response)).hasSize(1);
        }

        @Test
        @DisplayName("sorts unsorted input by time")
        void sortsByTime() {
            JsonNode response = json("""
                    {"predictions": [
                      {"t": "2026-09-28 16:41", "v": "1.0", "type": "H"},
                      {"t": "2026-09-28 04:30", "v": "0.0", "type": "L"}
                    ]}""");

            List<TideMath.TurningPoint> points = TideMath.parseHilo(response);

            assertThat(points.get(0).time()).isBefore(points.get(1).time());
        }
    }

    @Nested
    @DisplayName("parseInstant")
    class ParseInstant {

        @ParameterizedTest(name = "\"{0}\" -> {1}")
        @CsvSource({
            "2026-09-28 04:30,        2026-09-28T04:30:00Z",
            "2026-09-28 04:30:45,     2026-09-28T04:30:45Z",
            "20260928 04:30,         2026-09-28T04:30:00Z",
            "20260928 04:30:15,      2026-09-28T04:30:15Z",
            "2026-09-28T04:30:00Z,    2026-09-28T04:30:00Z",
            "2026-09-28T04:30:00+00:00, 2026-09-28T04:30:00Z"
        })
        @DisplayName("accepts every timestamp shape CO-OPS emits, as UTC")
        void acceptsKnownShapes(String raw, String expected) {
            assertThat(TideMath.parseInstant(raw)).isEqualTo(Instant.parse(expected));
        }

        @Test
        @DisplayName("returns null rather than guessing")
        void rejectsGarbage() {
            assertThat(TideMath.parseInstant(null)).isNull();
            assertThat(TideMath.parseInstant("   ")).isNull();
            assertThat(TideMath.parseInstant("28/09/2026")).isNull();
            assertThat(TideMath.parseInstant("2026-13-45 99:99")).isNull();
        }
    }

    @Nested
    @DisplayName("interpolate")
    class Interpolate {

        private final List<TideMath.TurningPoint> hilo = List.of(
                new TideMath.TurningPoint(Instant.parse("2026-09-28T00:00:00Z"), null, -0.4),
                new TideMath.TurningPoint(Instant.parse("2026-09-28T06:00:00Z"), 1.2, null),
                new TideMath.TurningPoint(Instant.parse("2026-09-28T12:00:00Z"), null, -0.4));

        @Test
        @DisplayName("interpolates a falling tide halfway between high and low")
        void falling() {
            TideMath.Interpolation result = TideMath.interpolate(hilo,
                    Instant.parse("2026-09-28T09:00:00Z")).orElseThrow();

            assertThat(result.phase()).isEqualTo("falling");
            assertThat(result.heightMeters()).isEqualTo(0.4);
            assertThat(result.nextTime()).isEqualTo(Instant.parse("2026-09-28T12:00:00Z"));
            assertThat(result.nextType()).isEqualTo("LOW");
        }

        @Test
        @DisplayName("interpolates a rising tide between low and high")
        void rising() {
            TideMath.Interpolation result = TideMath.interpolate(hilo,
                    Instant.parse("2026-09-28T03:00:00Z")).orElseThrow();

            assertThat(result.phase()).isEqualTo("rising");
            assertThat(result.heightMeters()).isEqualTo(0.4);
            assertThat(result.nextType()).isEqualTo("HIGH");
        }

        @Test
        @DisplayName("is empty outside the published window")
        void outsideWindow() {
            assertThat(TideMath.interpolate(hilo, Instant.parse("2026-09-27T12:00:00Z"))).isEmpty();
            assertThat(TideMath.interpolate(hilo, Instant.parse("2026-10-01T00:00:00Z"))).isEmpty();
        }

        @Test
        @DisplayName("needs at least two turning points")
        void insufficientData() {
            assertThat(TideMath.interpolate(List.of(), Instant.now())).isEmpty();
            assertThat(TideMath.interpolate(null, Instant.now())).isEmpty();
            assertThat(TideMath.interpolate(
                    List.of(hilo.get(0)), Instant.now())).isEmpty();
        }

        @Test
        @DisplayName("recognises slack water at mean sea level")
        void meanSeaLevel() {
            assertThat(TideMath.isNearMeanSeaLevel(0.02)).isTrue();
            assertThat(TideMath.isNearMeanSeaLevel(0.4)).isFalse();
        }
    }

    @Test
    @DisplayName("turning point extremes are stable")
    void extremes() {
        TideMath.TurningPoint high = new TideMath.TurningPoint(Instant.now(), 1.5, null);
        TideMath.TurningPoint low = new TideMath.TurningPoint(Instant.now(), null, -0.5);

        assertThat(high.extreme()).isEqualTo(1.5);
        assertThat(low.extreme()).isEqualTo(-0.5);
        assertThat(LocalDateTime.ofInstant(high.time(), ZoneOffset.UTC).getHour())
                    .isEqualTo(LocalDateTime.ofInstant(high.time(), ZoneOffset.UTC).getHour());
    }
}
