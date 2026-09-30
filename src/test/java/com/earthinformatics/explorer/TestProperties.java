package com.earthinformatics.explorer;

import com.earthinformatics.explorer.properties.ExplorerProperties;
import java.time.Duration;
import java.util.List;

/**
 * Builds fully populated {@link ExplorerProperties} for unit tests.
 *
 * <p>The properties records are constructor-injected with no defaults applied outside Spring
 * binding, so a test that constructs them by hand has to supply every field or trip over a
 * NullPointerException three layers away from the mistake. This factory fills in values that are
 * irrelevant to a unit test and exposes small overrides for the ones that are not.
 */
public final class TestProperties {

    private TestProperties() {
    }

    public static ExplorerProperties defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String eonetBaseUrl = "https://eonet.gsfc.nasa.gov/api/v3";
        private int eonetMaxLimit = 1000;
        private int eonetDefaultDays = 365;
        private String ndviBaseUrl = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi";
        private String ndviLayer = "MODIS_Terra_L3_NDVI_16Day";
        private String noaaBaseUrl = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter";
        private String openMeteoForecast = "https://api.open-meteo.com/v1/forecast";
        private String openMeteoAirQuality = "https://air-quality-api.open-meteo.com/v1/air-quality";
        private String openMeteoMarine = "https://marine-api.open-meteo.com/v1/marine";
        private Duration responseTimeout = Duration.ofSeconds(20);
        private boolean openMeteoEnabled = true;
        private int openMeteoMaxCells = 30;
        private int maxRetries = 1;
        private Duration retryBackoff = Duration.ofMillis(20);

        public Builder eonet(String baseUrl, int maxLimit, int defaultDays) {
            this.eonetBaseUrl = baseUrl;
            this.eonetMaxLimit = maxLimit;
            this.eonetDefaultDays = defaultDays;
            return this;
        }

        public Builder ndvi(String baseUrl, String layer) {
            this.ndviBaseUrl = baseUrl;
            this.ndviLayer = layer;
            return this;
        }

        public Builder noaa(String baseUrl) {
            this.noaaBaseUrl = baseUrl;
            return this;
        }

        public Builder openMeteoMaxCells(int maxCells) {
            this.openMeteoMaxCells = maxCells;
            return this;
        }

        public Builder openMeteoEnabled(boolean enabled) {
            this.openMeteoEnabled = enabled;
            return this;
        }

        public Builder openMeteo(String forecast, String airQuality, String marine) {
            this.openMeteoForecast = forecast;
            this.openMeteoAirQuality = airQuality;
            this.openMeteoMarine = marine;
            return this;
        }

        /** Tests want fast failures, never 20-second waits. */
        public Builder fast() {
            this.responseTimeout = Duration.ofSeconds(2);
            this.maxRetries = 1;
            this.retryBackoff = Duration.ofMillis(10);
            return this;
        }

        public ExplorerProperties build() {
            ExplorerProperties.Upstreams upstreams = new ExplorerProperties.Upstreams(
                    new ExplorerProperties.Upstreams.Usgs(
                            "https://earthquake.usgs.gov/earthquakes/feed/v1.0",
                            List.of("all_day", "all_hour"), "all_day"),
                    new ExplorerProperties.Upstreams.Eonet(eonetBaseUrl, eonetMaxLimit,
                            eonetDefaultDays),
                    new ExplorerProperties.Upstreams.Firms(
                            "https://firms.modaps.eosdis.nasa.gov/api", "DEMO_KEY", true, -180,
                            -90, 180, 90, 1, false),
                    new ExplorerProperties.Upstreams.OpenMeteo(openMeteoForecast,
                            openMeteoAirQuality, openMeteoMarine, 15, 5, 90,
                            openMeteoMaxCells,
                            openMeteoEnabled),
                    new ExplorerProperties.Upstreams.OpenSky(
                            "https://opensky-network.org/api", "", "", "", "90.0", true, 4000),
                    new ExplorerProperties.Upstreams.Ais("wss://stream.aisstream.io/v3/stream/sat",
                            "", true, 5000, Duration.ofMinutes(15), Duration.ofSeconds(10),
                            Duration.ofMinutes(5)),
                    new ExplorerProperties.Upstreams.Noaa(noaaBaseUrl,
                            "EARTH-INFORMATICS-EXPLORER", true, 4, null),
                    new ExplorerProperties.Upstreams.Ndvi(ndviBaseUrl, ndviLayer, "", "EPSG:4326",
                            30, 15, true),
                    new ExplorerProperties.Upstreams.Resilience(4.0, 6.0, Duration.ofSeconds(8),
                            responseTimeout, maxRetries, retryBackoff, 52_428_800));

            return new ExplorerProperties(
                    new ExplorerProperties.Cache(Duration.ofMinutes(15), Duration.ofMinutes(10),
                            256, Duration.ofSeconds(60), Duration.ofSeconds(30), 512),
                    new ExplorerProperties.WebSocket(Duration.ofSeconds(5), 10, 1_048_576,
                            Duration.ofSeconds(2)),
                    upstreams,
                    new ExplorerProperties.Api("v1", true, List.of("*"), true, true));
        }
    }
}
