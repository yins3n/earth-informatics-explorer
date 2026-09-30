package com.earthinformatics.explorer.properties;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Root of the externalised configuration. Bound from {@code explorer.*} in application.yml.
 *
 * <p>Every value has a safe default so the service boots with zero configuration; operators
 * only need to supply credentials for the providers that demand them (NASA FIRMS, OpenSky).
 */
@ConfigurationProperties(prefix = "explorer")
public record ExplorerProperties(

        Cache cache,
        WebSocket websocket,
        Upstreams upstreams,
        Api api) {

    // ------------------------------------------------------------------ cache

    /**
     * @param defaultTtl       TTL applied to dataset caches that have no explicit spec.
     * @param defaultMaxSize   Entry ceiling for dynamically created caches.
     * @param shortTtl         TTL used for aggregates and kinetic layers (vessels, aircraft).
     * @param telemetryTtl     TTL for the pre-aggregated summaries consumed by the WS tick.
     * @param singleFlight     Maximum number of concurrently tracked in-flight de-duplications.
     */
    public record Cache(
            @DefaultValue("15m") Duration defaultTtl,
            @DefaultValue("10m") Duration defaultRefreshAfterWrite,
            @DefaultValue("256") long defaultMaxSize,
            @DefaultValue("60s") Duration shortTtl,
            @DefaultValue("30s") Duration telemetryTtl,
            @DefaultValue("512") int singleFlight) {
    }

    // -------------------------------------------------------------- websocket

    /**
     * @param tickInterval  How often a telemetry tick is computed and broadcast.
     * @param bufferTicks   Ticks retained for slow clients before the client is disconnected.
     */
    public record WebSocket(
            @DefaultValue("5s") Duration tickInterval,
            @DefaultValue("10") int bufferTicks,
            @DefaultValue("1048576") int maxInboundMessageSize,
            @DefaultValue("2s") Duration sendTimeout) {
    }

    // --------------------------------------------------------------- upstream

    public record Upstreams(
            Usgs usgs,
            Eonet eonet,
            Firms firms,
            OpenMeteo openMeteo,
            OpenSky openSky,
            Ais ais,
            Noaa noaa,
            Ndvi ndvi,
            Resilience resilience) {

        /** USGS Earthquake Hazards Program real-time feeds. */
        public record Usgs(
                @DefaultValue("https://earthquake.usgs.gov/earthquakes/feed/v1.0") String baseUrl,
                @DefaultValue("all_hour,all_day") List<String> feeds,
                @DefaultValue("all_day") String defaultFeed) {
        }

        /** NASA EONET v3 natural-hazard event API. */
        public record Eonet(
                @DefaultValue("https://eonet.gsfc.nasa.gov/api/v3") String baseUrl,
                @DefaultValue("1000") int maxLimit,
                @DefaultValue("365") int defaultDays) {
        }

        /**
         * NASA FIRMS active-fire API.
         *
         * @param mapKey    Free MAP_KEY. {@code DEMO_KEY} works but is heavily throttled and
         *                  limited to small bounding boxes, so production deployments must
         *                  register a key at https://firms.modaps.eosdis.nasa.gov/mapkey/
         */
        public record Firms(
                @DefaultValue("https://firms.modaps.eosdis.nasa.gov/api") String baseUrl,
                @DefaultValue("DEMO_KEY") String mapKey,
                @DefaultValue("true") boolean enabled,
                @DefaultValue("180") double minLongitude,
                @DefaultValue("-90") double minLatitude,
                @DefaultValue("180") double maxLongitude,
                @DefaultValue("90") double maxLatitude,
                @DefaultValue("1") int days,
                @DefaultValue("false") boolean csvFallbackEnabled) {
        }

        /** Open-Meteo forecast / air-quality / marine APIs. */
        public record OpenMeteo(
                @DefaultValue("https://api.open-meteo.com/v1") String forecastUrl,
                @DefaultValue("https://air-quality-api.open-meteo.com/v1") String airQualityUrl,
                @DefaultValue("https://marine-api.open-meteo.com/v1") String marineUrl,
                @DefaultValue("15") double gridStepDegrees,
                @DefaultValue("5") double minGridStepDegrees,
                @DefaultValue("90") double maxGridStepDegrees,
                // 100 locations per request is the most Open-Meteo's free tier serves reliably
                // in one call. A smaller chunk size is not "safer", it is just more requests:
                // at 30 a single air-quality lattice cost 87 calls, and four refresh cycles an
                // hour (the cache TTL) exhausted the hourly quota on its own, which showed up
                // as "lattice chunks were rejected" rather than as a rate-limit message.
                @DefaultValue("100") int maxCellsPerChunk,
                @DefaultValue("true") boolean enabled) {
        }

        /**
         * OpenSky Network state-vector API. Anonymous access is free but heavily throttled;
         * supplying OAuth2 client credentials raises the allowance substantially.
         */
        public record OpenSky(
                @DefaultValue("https://opensky-network.org/api") String baseUrl,
                @DefaultValue("https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token") String tokenUrl,
                @DefaultValue("") String clientId,
                @DefaultValue("") String clientSecret,
                @DefaultValue("90.0") String audience,
                @DefaultValue("true") boolean enabled,
                @DefaultValue("4000") int maxStates) {
        }

        /**
         * AISStream.io live vessel AIS feed (websocket push).
         *
         * @param enabled  When false the vessel endpoint reports a degraded/empty collection
         *                 instead of dialling out; useful for air-gapped or offline demos.
         */
        public record Ais(
                @DefaultValue("wss://stream.aisstream.io/v3/stream/sat") String url,
                @DefaultValue("") String apiKey,
                @DefaultValue("true") boolean enabled,
                @DefaultValue("5000") int maxTrackedVessels,
                @DefaultValue("15m") Duration vesselTtl,
                @DefaultValue("10s") Duration reconnectDelay,
                @DefaultValue("5m") Duration reconnectMaxDelay) {
        }

        /** NOAA CO-OPS (NDBC/ACOM) tides & water-temperature API. */
        public record Noaa(
                @DefaultValue("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter") String baseUrl,
                @DefaultValue("EARTH-INFORMATICS-EXPLORER") String application,
                @DefaultValue("true") boolean enabled,
                @DefaultValue("3") int maxConcurrentRequests,
                /**
                 * Optional override of the built-in station set. When left unset the client uses
                 * {@code TideStations#DEFAULT}; identifiers that NOAA no longer serves simply
                 * degrade to a per-station warning.
                 */
                List<Station> stations) {

            /** Configured station override entry. */
            public record Station(String id, String name, String country, double latitude,
                    double longitude) {
            }
        }

        /**
         * Normalised Difference Vegetation Index. Served as a WMS {@code GetFeatureInfo} proxy
         * against any compliant imagery server (NASA GIBS by default).
         */
        public record Ndvi(
                @DefaultValue("https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi") String baseUrl,
                @DefaultValue("MODIS_Terra_L3_NDVI_16Day") String layerName,
                @DefaultValue("") String style,
                @DefaultValue("EPSG:4326") String srs,
                @DefaultValue("30") int maxFeatures,
                @DefaultValue("15") double gridStepDegrees,
                @DefaultValue("true") boolean enabled) {
        }

        /**
         * Shared resilience policy: per-host token buckets, timeouts, retries, jitter.
         *
         * @param tokensPerSecond  Sustained request rate allowed per upstream host.
         * @param burst            Bucket capacity, i.e. the largest instantaneous fan-out.
         */
        public record Resilience(
                @DefaultValue("4.0") double tokensPerSecond,
                @DefaultValue("6.0") double burst,
                @DefaultValue("8s") Duration connectTimeout,
                @DefaultValue("20s") Duration responseTimeout,
                @DefaultValue("2") int maxRetries,
                @DefaultValue("400ms") Duration retryBackoff,
                @DefaultValue("52428800") int maxResponseBytes) {
        }
    }

    // -------------------------------------------------------------------- api

    public record Api(
            @DefaultValue("v1") String version,
            @DefaultValue("true") boolean corsEnabled,
            @DefaultValue("*") List<String> allowedOrigins,
            @DefaultValue("true") boolean exposeProvenance,
            @DefaultValue("true") boolean enabled) {
    }

    /** Convenience accessor: resilience policy applies to every upstream, not per provider. */
    public Upstreams.Resilience resilience() {
        return upstreams.resilience();
    }
}
