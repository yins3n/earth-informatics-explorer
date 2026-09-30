package com.earthinformatics.explorer.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Geometry helpers shared by every domain service.
 *
 * <p>All methods are pure/static and unit tested; nothing here touches the event loop.
 */
public final class Geo {

    public static final double EARTH_RADIUS_KM = 6371.0088;
    private static final double DEG2RAD = Math.PI / 180.0;
    private static final double RAD2DEG = 180.0 / Math.PI;

    private Geo() {
    }

    /**
     * Latitude/longitude bounding box.
     *
     * <p>West greater than east is legal and means the box <em>wraps the antimeridian</em>, which
     * is what a viewer sees when they pan across 180 degrees. {@link #contains(double, double)}
     * handles both cases, and {@link #centreLongitude()} still yields a sensible midpoint (0, for
     * the 170E/-170 case) because the arithmetic works out even when the width is negative.
     */
    public record BBox(double west, double south, double east, double north) {

        public BBox {
            if (south > north) {
                throw new IllegalArgumentException("south must not exceed north");
            }
            if (Math.abs(south) > 90 || Math.abs(north) > 90) {
                throw new IllegalArgumentException("latitude must be within [-90, 90]");
            }
            if (Math.abs(west) > 360 || Math.abs(east) > 360) {
                throw new IllegalArgumentException("longitude must be within [-360, 360]");
            }
        }

        /** True when the box crosses the antimeridian. */
        public boolean isWrapped() {
            return west > east;
        }

        public static BBox global() {
            return new BBox(-180, -90, 180, 90);
        }

        public boolean contains(double lon, double lat) {
            boolean inLon = west <= east ? (lon >= west && lon <= east)
                    : (lon >= west || lon <= east);
            return inLon && lat >= south && lat <= north;
        }

        public double widthDegrees() {
            return Math.abs(east - west);
        }

        public double heightDegrees() {
            return north - south;
        }

        /** Nominal centre, used for label placement and single-point provider requests. */
        public double centreLongitude() {
            double mid = west + (east - west) / 2.0;
            return mid > 180 ? mid - 360 : mid;
        }

        public double centreLatitude() {
            return south + (north - south) / 2.0;
        }

        /** Rounds the box outward to a whole degree, which keeps upstream cache keys tidy. */
        public BBox snapped() {
            return new BBox(Math.floor(west), Math.floor(south), Math.ceil(east), Math.ceil(north));
        }
    }

    /** One cell of a regular lat/lon lattice, anchored at the cell centre. */
    public record GridCell(int row, int column, double latitude, double longitude) {
    }

    /**
     * Builds a regular lattice covering the whole sphere.
     *
     * <p>Cell centres are offset by half a step so the lattice is symmetric about the equator
     * and the antimeridian - important because a cell centred exactly on +/-180 degrees would
     * be rendered by Cesium as a degenerate polygon.
     *
     * @param stepDegrees  Cell size in degrees; must be &gt; 0.
     */
    public static List<GridCell> grid(double stepDegrees) {
        if (stepDegrees <= 0) {
            throw new IllegalArgumentException("grid step must be positive");
        }
        List<GridCell> cells = new ArrayList<>();
        int columns = (int) Math.ceil(360.0 / stepDegrees);
        // Latitude row centres never coincide with the poles, keeping cell area well behaved.
        int rows = (int) Math.ceil(180.0 / stepDegrees);
        for (int row = 0; row < rows; row++) {
            double latitude = -90.0 + (row + 0.5) * (180.0 / rows);
            if (latitude > 90.0) {
                latitude = 90.0;
            }
            for (int column = 0; column < columns; column++) {
                double longitude = -180.0 + (column + 0.5) * (360.0 / columns);
                if (longitude >= 180.0) {
                    longitude -= 360.0;
                }
                cells.add(new GridCell(row, column, latitude, longitude));
            }
        }
        return cells;
    }

    /** Splits a list into fixed-size chunks; used to respect Open-Meteo's multi-location limit. */
    public static <T> List<List<T>> chunk(List<T> source, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("chunk size must be positive");
        }
        List<List<T>> chunks = new ArrayList<>((source.size() + size - 1) / size);
        for (int start = 0; start < source.size(); start += size) {
            chunks.add(List.copyOf(source.subList(start, Math.min(start + size, source.size()))));
        }
        return chunks;
    }

    /** Great-circle distance in kilometres. */
    public static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        double dLat = (lat2 - lat1) * DEG2RAD;
        double dLon = (lon2 - lon1) * DEG2RAD;
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(lat1 * DEG2RAD) * Math.cos(lat2 * DEG2RAD)
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return 2 * EARTH_RADIUS_KM * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }

    /** Normalises a longitude into [-180, 180). */
    public static double wrapLongitude(double longitude) {
        double wrapped = (longitude + 180.0) % 360.0;
        if (wrapped < 0) {
            wrapped += 360.0;
        }
        return wrapped - 180.0;
    }

    public static boolean isValidLatitude(double latitude) {
        return !Double.isNaN(latitude) && latitude >= -90 && latitude <= 90;
    }

    public static boolean isValidLongitude(double longitude) {
        return !Double.isNaN(longitude) && longitude >= -180 && longitude <= 180;
    }

    /** True when the coordinate pair is a usable WGS84 position. */
    public static boolean isValidPosition(double longitude, double latitude) {
        return isValidLongitude(longitude) && isValidLatitude(latitude);
    }

    /** Compass bearing label ("NNW") for a wind/current direction in degrees. */
    public static String compassLabel(double degrees) {
        if (Double.isNaN(degrees)) {
            return "N/A";
        }
        String[] points = {"N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"};
        int index = (int) Math.floor(((degrees % 360) + 360) % 360 / 22.5 + 0.5) % 16;
        return points[index];
    }
}
