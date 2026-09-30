package com.earthinformatics.explorer.util;

import com.earthinformatics.explorer.dto.TideStation;
import java.util.List;

/**
 * Default global tide-prediction station set.
 *
 * <p>Every identifier below was verified against the live
 * {@code api/prod/datagetter?product=predictions} endpoint, because a station id that NOAA has
 * decommissioned answers HTTP 400 and would otherwise silently contribute an empty layer. The
 * network is chosen for global spread: the North Atlantic, North Pacific, equatorial Pacific,
 * Indian Ocean and Caribbean.
 *
 * <p>The set is deliberately capped because the CO-OPS datagetter allows only 3 requests/second
 * and 500/day per client without registration - a globe-wide station network would need a paid
 * plan or an aggregator. Sixteen stations every 5 minutes is ~4,600 requests/day, which is why
 * {@code explorer.upstreams.noaa.enabled} and the 5-15 minute cache tier matter: they are the
 * difference between working and being rate-limited.
 *
 * <p>Override with {@code explorer.upstreams.noaa.stations} in application.yml; unknown or
 * decommissioned identifiers degrade gracefully rather than failing the whole request.
 */
public final class TideStations {

    public static final List<TideStation> DEFAULT = List.of(
            // --- North Pacific -------------------------------------------------
            new TideStation("9414290", "San Francisco, CA", "US", 37.8065, -122.4669),
            new TideStation("9447130", "Seattle, WA", "US", 47.6026, -122.3393),
            new TideStation("9455920", "Anchorage, AK", "US", 61.2375, -149.8904),
            new TideStation("1612340", "Honolulu, HI", "US", 21.3033, -157.8645),
            new TideStation("1611400", "Nawiliwili, HI", "US", 21.9545, -159.3561),
            new TideStation("1617760", "Hilo, HI", "US", 19.7303, -155.0556),
            // --- North Atlantic ------------------------------------------------
            new TideStation("8443970", "Boston, MA", "US", 42.3539, -71.0503),
            new TideStation("8518750", "The Battery, NY", "US", 40.7006, -74.0142),
            new TideStation("8638610", "Willimantic, CT", "US", 41.3034, -72.0930),
            new TideStation("8729108", "Panama City, FL", "US", 30.1497, -85.6644),
            new TideStation("8723214", "Charleston, SC", "US", 32.7665, -79.9250),
            new TideStation("8745557", "Gulfport Harbor, MS", "US", 30.3600, -89.0817),
            // --- Caribbean / Central America -----------------------------------
            new TideStation("9755371", "San Juan, PR", "PR", 18.4589, -66.1164),
            new TideStation("9812501", "Balboa, Panama", "PA", 8.9667, -79.5667),
            // --- Equatorial Pacific and Indian Ocean --------------------------
            new TideStation("1732417", "Papeete, Tahiti", "PF", -17.5350, -149.5720),
            new TideStation("1630000", "Apra Harbor, Guam", "GU", 13.4434, 144.6564),
            new TideStation("1820000", "Kwajalein Atoll", "MH", 8.7317, 167.7361),
            new TideStation("2431000", "Diego Garcia", "IO", -7.2900, 72.3933),
            new TideStation("6835001", "Jakarta, Java", "ID", -2.2017, 106.8667),
            new TideStation("2695535", "Bermuda", "BM", 32.3702, -64.6957));

    private TideStations() {
    }

    /** Bounding box containing every default station; used for viewport culling. */
    public static Geo.BBox boundingBox() {
        double west = DEFAULT.stream().mapToDouble(TideStation::longitude).min().orElse(-180);
        double east = DEFAULT.stream().mapToDouble(TideStation::longitude).max().orElse(180);
        double south = DEFAULT.stream().mapToDouble(TideStation::latitude).min().orElse(-90);
        double north = DEFAULT.stream().mapToDouble(TideStation::latitude).max().orElse(90);
        return new Geo.BBox(west, south, east, north);
    }
}
