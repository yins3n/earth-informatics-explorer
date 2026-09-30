package com.earthinformatics.explorer.dto;

/**
 * A NOAA CO-OPS tide prediction station.
 *
 * @param id        Station identifier used by {@code api/prod/datagetter?station=...}.
 * @param name      Display name.
 * @param country   ISO-ish country label.
 * @param latitude  Station latitude, degrees north.
 * @param longitude Station longitude, degrees east.
 */
public record TideStation(String id, String name, String country, double latitude,
                          double longitude) {

    /** Sanity check used before dialling NOAA - a mis-typed coordinate wastes a rate-limit slot. */
    public boolean isValid() {
        return id != null && !id.isBlank()
                && com.earthinformatics.explorer.util.Geo.isValidPosition(longitude, latitude);
    }
}
