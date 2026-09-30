package com.earthinformatics.explorer.util;

import com.earthinformatics.explorer.error.InvalidRequestException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Query-parameter parsing shared by every controller.
 *
 * <p>Centralised for one reason: the validation messages are the contract the frontend's error
 * banner shows to users, so they need to be consistent. Every failure raises
 * {@link InvalidRequestException}, which the global handler renders as HTTP 400 with the
 * offending parameter named.
 */
public final class QueryParameters {

    private QueryParameters() {
    }

    /**
     * Parses {@code west,south,east,north}.
     *
     * <p>A blank or absent value means "no viewport", i.e. the whole globe. Note that
     * {@code west > east} is deliberately accepted: that is a box wrapping the antimeridian, not
     * a mistake, and a user who pans across 180 degrees produces one on every frame.
     */
    public static Geo.BBox bbox(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String[] parts = raw.split(",");
        if (parts.length != 4) {
            throw new InvalidRequestException(
                    "bbox must be 'west,south,east,north' (4 comma-separated numbers) but was '"
                            + raw + "'");
        }
        try {
            return new Geo.BBox(
                    Double.parseDouble(parts[0].trim()),
                    Double.parseDouble(parts[1].trim()),
                    Double.parseDouble(parts[2].trim()),
                    Double.parseDouble(parts[3].trim()));
        } catch (NumberFormatException notANumber) {
            throw new InvalidRequestException("bbox contains a non-numeric value: " + raw);
        } catch (IllegalArgumentException invalid) {
            throw new InvalidRequestException("bbox is out of range: " + invalid.getMessage());
        }
    }

    /**
     * Validates a numeric parameter, substituting the default when absent.
     *
     * <p>Out-of-range values are rejected rather than clamped: silently substituting a
     * different lattice than the client asked for produces a map that is wrong without any
     * signal that it is wrong, which is worse than a 400.
     */
    public static double doubleInRange(String name, Double raw, double fallback, double min,
            double max) {
        if (raw == null || raw.isNaN()) {
            return fallback;
        }
        double value = raw;
        if (value < min || value > max) {
            throw new InvalidRequestException(
                    name + " must be between " + trim(min) + " and " + trim(max) + " but was "
                            + trim(value));
        }
        return value;
    }

    /**
     * Validates an integer parameter, substituting the default when absent.
     *
     * <p>As with {@link #doubleInRange}, out-of-range values are rejected, not clamped.
     */
    public static int intInRange(String name, Integer raw, int fallback, int min, int max) {
        if (raw == null) {
            return fallback;
        }
        if (raw < min || raw > max) {
            throw new InvalidRequestException(
                    name + " must be between " + min + " and " + max + " but was " + raw);
        }
        return raw;
    }

    /** Normalises an optional string, mapping blank to {@code null}. */
    public static String optional(String raw) {
        return raw == null || raw.isBlank() ? null : raw.strip().toLowerCase(Locale.ROOT);
    }

    /** Enumerates a constrained string parameter, listing the legal values on failure. */
    public static String oneOf(String name, String raw, String fallback, String... allowed) {
        String value = raw == null || raw.isBlank() ? fallback
                : raw.strip().toLowerCase(Locale.ROOT);
        Set<String> legal = new LinkedHashSet<>(Arrays.asList(allowed));
        if (!legal.contains(value)) {
            throw new InvalidRequestException(
                    name + " must be one of " + legal + " but was '" + value + "'");
        }
        return value;
    }

    /** Renders a double without a trailing {@code .0} for tidier error messages. */
    private static String trim(double value) {
        return value == Math.rint(value) ? String.valueOf((long) value) : String.valueOf(value);
    }
}
