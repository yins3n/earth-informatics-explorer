package com.earthinformatics.explorer.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Tide mathematics.
 *
 * <p>NOAA's {@code interval=hilo} product publishes only the turning points (high/low water),
 * roughly two per day, which is far too coarse to animate on a globe. This class reconstructs a
 * continuous height curve by linear interpolation between consecutive turning points - the same
 * first-order approximation used by tide-prediction displays, and accurate to a few centimetres
 * in the mid-tide region (error concentrates exactly at the turning points, where the true curve
 * is flat, so the visual error is negligible).
 *
 * <p>Pure functions, fully unit tested, no Reactor types: this is numeric code and should be
 * verifiable by reading it.
 */
public final class TideMath {

    private static final double MSL_TOLERANCE = 0.05;

    /** Timestamp shapes observed across CO-OPS products, tried in order. */
    private static final List<DateTimeFormatter> TIMESTAMP_FORMATS = List.of(
            DateTimeFormatter.ofPattern("yyyyMMdd HH:mm"),
            DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ISO_LOCAL_DATE_TIME);

    private TideMath() {
    }

    /** One turning point from the NOAA hilo table. */
    public record TurningPoint(Instant time, Double high, Double low) {

        public boolean isHigh() {
            return high != null && !high.isNaN();
        }

        public boolean isLow() {
            return low != null && !low.isNaN();
        }

        public double extreme() {
            return isHigh() ? high : low;
        }

        public String type() {
            return isHigh() ? "HIGH" : "LOW";
        }
    }

    /**
     * Interpolated state of the tide at a point in time.
     *
     * @param heightMeters Height above mean sea level, metres.
     * @param phase        {@code rising}, {@code falling} or {@code slack}.
     * @param nextTime     Time of the next turning point.
     * @param nextHeight   Height of the next turning point.
     */
    public record Interpolation(double heightMeters, String phase, Instant nextTime,
            double nextHeight, String nextType) {
    }

    /**
     * Parses a NOAA hilo response.
     *
     * <p>CO-OPS publishes two different shapes for the same data and the choice is the caller's:
     * <ul>
     *   <li><b>Current</b> ({@code api/prod/datagetter?product=predictions&format=json}):
     *       {@code {"predictions":[{"t":"2026-09-28 04:30","v":"-0.158","type":"L"}, ...]}} -
     *       one row per turning point, the value and its kind in separate fields.</li>
     *   <li><b>Legacy</b> ({@code product=predictions&format=json, no hilo}):
     *       {@code {"data":[{"v_date":"20260928 04:30","v_hi":"1.2","v_lo":""}, ...]}} - one row
     *       per interval with both extremes on the same row.</li>
     * </ul>
     * Both are accepted, and the modern shape is tried first because that is what the client
     * actually requests. Supporting only one is a silent failure: the parse returns an empty list,
     * every station is counted as failed, and the tide layer renders empty while the API logs
     * nothing.
     */
    public static List<TurningPoint> parseHilo(JsonNode response) {
        List<TurningPoint> points = new ArrayList<>();
        if (response == null) {
            return points;
        }
        JsonNode modern = response.path("predictions");
        if (modern.isArray()) {
            for (JsonNode row : modern) {
                Instant time = parseInstant(row.path("t").asText(null));
                Double value = optionalDouble(row, "v");
                if (time == null || value == null) {
                    // No value (or a "NaN" placeholder): the row carries no turning point.
                    continue;
                }
                // "H"/"HIGH" is a high water; anything else in the hilo product is a low water.
                String type = row.path("type").asText("");
                boolean high = "H".equalsIgnoreCase(type) || "HIGH".equalsIgnoreCase(type);
                points.add(high
                        ? new TurningPoint(time, value, null)
                        : new TurningPoint(time, null, value));
            }
            points.sort(Comparator.comparing(TurningPoint::time));
            return points;
        }

        JsonNode legacy = response.path("data");
        if (legacy.isArray()) {
            for (JsonNode row : legacy) {
                Instant time = parseInstant(row.path("v_date").asText(null));
                Double high = optionalDouble(row, "v_hi");
                Double low = optionalDouble(row, "v_lo");
                if (time == null || (high == null && low == null)) {
                    continue;
                }
                points.add(new TurningPoint(time, high, low));
            }
            points.sort(Comparator.comparing(TurningPoint::time));
        }
        return points;
    }

    /**
     * Interpolates the water height at {@code at}.
     *
     * @return empty when fewer than two turning points are available, or when {@code at} falls
     *         outside the published window.
     */
    public static Optional<Interpolation> interpolate(List<TurningPoint> points, Instant at) {
        if (points == null || points.size() < 2) {
            return Optional.empty();
        }
        for (int i = 0; i < points.size() - 1; i++) {
            TurningPoint current = points.get(i);
            TurningPoint next = points.get(i + 1);
            if (!current.isHigh() || !next.isLow()) {
                // The hilo feed alternates HIGH/LOW; anything else means a partial window.
                continue;
            }
            if (at.isBefore(current.time()) || at.isAfter(next.time())) {
                continue;
            }
            long spanMillis = Duration.between(current.time(), next.time()).toMillis();
            if (spanMillis <= 0) {
                continue;
            }
            double progress = (at.toEpochMilli() - current.time().toEpochMilli())
                    / (double) spanMillis;
            double height = current.extreme() + (next.extreme() - current.extreme()) * progress;
            return Optional.of(new Interpolation(round2(height), "falling", next.time(),
                    round2(next.extreme()), next.type()));
        }

        // Not inside a falling window: look for a rising one (the first row is sometimes a LOW).
        for (int i = 0; i < points.size() - 1; i++) {
            TurningPoint current = points.get(i);
            TurningPoint next = points.get(i + 1);
            if (!current.isLow() || !next.isHigh()) {
                continue;
            }
            if (at.isBefore(current.time()) || at.isAfter(next.time())) {
                continue;
            }
            long spanMillis = Duration.between(current.time(), next.time()).toMillis();
            if (spanMillis <= 0) {
                continue;
            }
            double progress = (at.toEpochMilli() - current.time().toEpochMilli())
                    / (double) spanMillis;
            double height = current.extreme() + (next.extreme() - current.extreme()) * progress;
            return Optional.of(new Interpolation(round2(height), "rising", next.time(),
                    round2(next.extreme()), next.type()));
        }
        return Optional.empty();
    }

    /** Convenience: true when a height is within a few centimetres of mean sea level. */
    public static boolean isNearMeanSeaLevel(double heightMeters) {
        return Math.abs(heightMeters) <= MSL_TOLERANCE;
    }

    /**
     * Parses a NOAA timestamp.
     *
     * <p>CO-OPS is not internally consistent about time formatting, and the same deployment
     * returns more than one shape depending on product and {@code time_zone}:
     * <ul>
     *   <li>{@code "20260928 04:30"} - compact local date plus 24h clock (legacy feed)</li>
     *   <li>{@code "2026-09-28 04:30"} - ISO-like with a <em>space</em> separator (current
     *       {@code product=predictions} feed, the one this service actually calls)</li>
     *   <li>{@code "2026-09-28T04:30:00Z"} / {@code "2026-09-28T04:30:00+00:00"} - full ISO
     *       instant</li>
     * </ul>
     * All are interpreted as UTC because the client always requests {@code time_zone=gmt}.
     * Missing seconds default to zero. Anything unparseable returns {@code null} so the caller
     * skips the row instead of inventing a time.
     */
    public static Instant parseInstant(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String value = raw.strip();
        for (DateTimeFormatter formatter : TIMESTAMP_FORMATS) {
            try {
                return LocalDateTime.from(formatter.parse(value))
                        .toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException notThisShape) {
                // Try the next known shape.
            }
        }
        try {
            // Full instants carry their own offset; LocalDateTime.from() rejects the trailing Z,
            // so parse as an OffsetDateTime first.
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException notAnInstant) {
            return null;
        }
    }

    private static Double optionalDouble(JsonNode row, String field) {
        JsonNode value = row.path(field);
        if (value.isNumber()) {
            return finite(value.asDouble());
        }
        if (value.isTextual() && !value.asText().isBlank()) {
            try {
                return finite(Double.parseDouble(value.asText().strip()));
            } catch (NumberFormatException notANumber) {
                return null;
            }
        }
        return null;
    }

    /**
     * Rejects non-finite readings.
     *
     * <p>CO-OPS renders a missing measurement as the literal string {@code "NaN"} in some feeds,
     * and {@code Double.parseDouble} happily turns that into a NaN. Keeping it would produce a
     * turning point whose {@code extreme()} is NaN, which propagates into the interpolated height
     * and then serialises as {@code "NaN"} in the payload - a value no renderer can draw.
     */
    private static Double finite(double value) {
        return Double.isNaN(value) || Double.isInfinite(value) ? null : value;
    }

    private static double round2(double value) {
        return Math.round(value * 100d) / 100d;
    }
}
