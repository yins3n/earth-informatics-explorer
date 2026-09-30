package com.earthinformatics.explorer.util;

import java.util.List;

/**
 * US EPA Air Quality Index breakpoints.
 *
 * <p>The renderer, the legend and the telemetry roll-up must agree on what "unhealthy" means, so
 * the classification lives here as a pure, unit-tested function instead of being re-implemented
 * in JavaScript.
 *
 * <p>Reference: US EPA AQI Technical Assistance Document, Table 3. The category <em>colours</em>
 * are the EPA published hex values, so a user comparing the globe with the official AQI map sees
 * the same thing.
 */
public final class AqiScale {

    /**
     * @param index     0-5, usable directly as a legend index.
     * @param label     EPA category name.
     * @param color     EPA hex colour.
     * @param advisory  Short human-readable implication.
     */
    public record Category(int index, String label, String color, String advisory) {
    }

    public static final List<Category> CATEGORIES = List.of(
            new Category(0, "good", "#00e400", "Air quality is satisfactory."),
            new Category(1, "moderate", "#ffff00", "Acceptable; unusually sensitive people should "
                    + "consider reducing prolonged exertion."),
            new Category(2, "unhealthy-sensitive", "#ff7e00", "Sensitive groups may experience "
                    + "health effects."),
            new Category(3, "unhealthy", "#ff0000", "Everyone may begin to experience health "
                    + "effects."),
            new Category(4, "very-unhealthy", "#8f3f97", "Health alert: the risk of health effects "
                    + "is increased for everyone."),
            new Category(5, "hazardous", "#7e0023", "Emergency conditions: the entire population "
                    + "is more likely to be affected."));

    private AqiScale() {
    }

    /** Maps a US AQI value onto its EPA category; out-of-range input clamps to the extremes. */
    public static Category classify(double usAqi) {
        if (Double.isNaN(usAqi) || usAqi <= 50) {
            return CATEGORIES.get(0);
        }
        if (usAqi <= 100) {
            return CATEGORIES.get(1);
        }
        if (usAqi <= 150) {
            return CATEGORIES.get(2);
        }
        if (usAqi <= 200) {
            return CATEGORIES.get(3);
        }
        if (usAqi <= 300) {
            return CATEGORIES.get(4);
        }
        return CATEGORIES.get(5);
    }

    /** Compact legend string embedded in {@code meta} so the UI never hard-codes the scale. */
    public static String legend() {
        StringBuilder legend = new StringBuilder();
        for (Category category : CATEGORIES) {
            if (!legend.isEmpty()) {
                legend.append('|');
            }
            legend.append(category.index()).append(' ').append(category.label())
                    .append(' ').append(category.color());
        }
        return legend.toString();
    }
}
