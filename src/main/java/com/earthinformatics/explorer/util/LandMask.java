package com.earthinformatics.explorer.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Bundled 1-degree land/sea mask.
 *
 * <h2>Why this exists</h2>
 * <p>Open-Meteo's marine product serves ocean variables only, and it rejects a multi-location
 * request with <em>HTTP 400 "No data is available for this location"</em> if <em>any</em>
 * coordinate in the batch sits on land. Sampling a global lat/lon lattice therefore fails in its
 * entirety the moment one cell is inland - which is always, because roughly a third of the world
 * is land. The alternative workarounds are both bad: issuing one request per cell multiplies the
 * request count by an order of magnitude, and bisecting failed chunks turns a 5-request refresh
 * into 40.
 *
 * <p>So the filter has to happen client-side, which needs to know where the coast is. Options
 * were a heavyweight land-polygon library, a runtime network call, or an 8 KB bitmask. The
 * bitmask wins on all three counts: no dependency, no network on the request path, and an O(1)
 * lookup that costs a bit test.
 *
 * <h2>Data</h2>
 * <p>Rasterised from Natural Earth 1:110m physical land polygons, packed one bit per cell,
 * row-major from 90N to 90S and 180W to 180E. Resolution of one degree is deliberately coarse:
 * the decision being made is "should this cell be sent to an ocean-only API", and a
 * conservative cell either way costs at most a slightly ragged coastline. Fine enough to keep
 * inland cells out of the request, coarse enough that a cell centred just offshore is still
 * kept.
 */
@Slf4j
public final class LandMask {

    /** Resource path of the packed mask. */
    public static final String RESOURCE = "/landmask-1deg.bin";

    private static final int WIDTH = 360;
    private static final int HEIGHT = 180;

    /**
     * The mask, or {@code null} if the resource could not be read.
     *
     * <p>Null is tolerated on purpose: a missing mask degrades to "assume everything is ocean",
     * which restores the pre-mask behaviour (a few failed chunks) instead of taking the
     * application down at startup.
     */
    private static final byte[] BITS = load();

    private LandMask() {
    }

    private static byte[] load() {
        try (InputStream in = LandMask.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                log.warn("Land mask {} not found; ocean-only sampling will fall back to "
                        + "unfiltered lattices", RESOURCE);
                return null;
            }
            byte[] bits = in.readAllBytes();
            int expected = (WIDTH * HEIGHT + 7) / 8;
            if (bits.length != expected) {
                log.warn("Land mask {} is {} bytes, expected {}; ignoring it", RESOURCE,
                        bits.length, expected);
                return null;
            }
            return bits;
        } catch (IOException unreadable) {
            log.warn("Land mask {} could not be read: {}", RESOURCE, unreadable.toString());
            return null;
        }
    }

    /** True when the mask is available. */
    public static boolean available() {
        return BITS != null;
    }

    /**
     * True when the point is on land.
     *
     * <p>Coordinates are wrapped rather than rejected, so a cell centre a hair beyond 180E is
     * looked up at -179.5E instead of throwing.
     */
    public static boolean isLand(double longitude, double latitude) {
        if (BITS == null) {
            // No mask: report ocean and let the upstream decide, which is the old behaviour.
            return false;
        }
        if (Double.isNaN(longitude) || Double.isNaN(latitude)) {
            return false;
        }
        int lonIndex = (int) Math.floor(Geo.wrapLongitude(longitude) + 180.0);
        int latIndex = (int) Math.floor(90.0 - latitude);
        if (lonIndex < 0 || lonIndex >= WIDTH || latIndex < 0 || latIndex >= HEIGHT) {
            return false;
        }
        int bit = latIndex * WIDTH + lonIndex;
        return (BITS[bit >>> 3] & (0x80 >>> (bit & 7))) != 0;
    }

    /** True when the point is not land. */
    public static boolean isOcean(double longitude, double latitude) {
        return !isLand(longitude, latitude);
    }

    /** True when a cell centre falls on land, the test {@link #filterOcean} applies. */
    public static boolean isLand(Geo.GridCell cell) {
        return isLand(cell.longitude(), cell.latitude());
    }

    /** Keeps only the cells whose centre is at sea, preserving lattice order. */
    public static List<Geo.GridCell> filterOcean(List<Geo.GridCell> cells) {
        if (BITS == null) {
            return List.copyOf(cells);
        }
        List<Geo.GridCell> ocean = new ArrayList<>(cells.size() / 2);
        for (Geo.GridCell cell : cells) {
            if (isOcean(cell.longitude(), cell.latitude())) {
                ocean.add(cell);
            }
        }
        return List.copyOf(ocean);
    }

    /**
     * Splits a lattice into its ocean and land cells, so the caller can report how much of the
     * field was skipped instead of silently drawing a grid with holes in it.
     */
    public static Partition partition(List<Geo.GridCell> cells) {
        if (BITS == null) {
            return new Partition(List.copyOf(cells), List.of());
        }
        List<Geo.GridCell> ocean = new ArrayList<>();
        List<Geo.GridCell> land = new ArrayList<>();
        for (Geo.GridCell cell : cells) {
            if (isLand(cell)) {
                land.add(cell);
            } else {
                ocean.add(cell);
            }
        }
        return new Partition(List.copyOf(ocean), List.copyOf(land));
    }

    /**
     * Fraction of the sphere that is land, for the payload metadata.
     */
    public static double landFraction() {
        if (BITS == null) {
            return Double.NaN;
        }
        int land = 0;
        for (byte bits : BITS) {
            land += Integer.bitCount(bits & 0xFF);
        }
        return (double) land / (WIDTH * HEIGHT);
    }

    /** Result of {@link #partition(List)}: the ocean cells and the skipped land cells. */
    public record Partition(List<Geo.GridCell> ocean, List<Geo.GridCell> land) {

        public int total() {
            return ocean.size() + land.size();
        }

        public Optional<String> describe() {
            if (total() == 0) {
                return Optional.empty();
            }
            return Optional.of("oceanCells=" + ocean.size() + ",skippedLandCells=" + land.size());
        }
    }
}
