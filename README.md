# Earth Informatics Explorer

A Spring WebFlux service that serves five Earth-observation domains as strict GeoJSON, plus a
CesiumJS dashboard that draws them on a globe and streams live domain telemetry over a WebSocket.

| Domain | Layers | Upstream |
| --- | --- | --- |
| `tectonics` | earthquakes, active volcanoes | USGS, NASA EONET |
| `atmospherics` | wind & weather, air quality | Open-Meteo |
| `oceans` | tides, currents, sea-surface temperature, water temperature | NOAA CO-OPS, Open-Meteo Marine |
| `biosphere` | wildfires, vegetation (NDVI) | NASA FIRMS, NASA GIBS WMS |
| `human-impact` | live aircraft, live vessels | OpenSky Network, AISStream.io |

## Quick start

```bash
./scripts/eie.sh run
```

That builds with the Maven wrapper, starts the server, waits for it to answer
`/api/v1/system/info`, and opens <http://localhost:8080>.

Then:

```bash
./scripts/eie.sh status      # is it up, on which port, since when
./scripts/eie.sh logs -f     # follow the log
./scripts/eie.sh stop        # graceful shutdown
./scripts/eie.sh restart
./scripts/eie.sh help        # every subcommand
```

### Requirements

- **JDK 17 or newer.** The default `java` on some systems is much newer than the build targets,
  so the scripts look for a 17 install under `/usr/lib/jvm` and fall back to `JAVA_HOME`. Set
  `JAVA_HOME` explicitly to override.
- **Maven** is not required on `PATH`; the bundled `./mvnw` wrapper is used when present.

### Configuration

All settings have working defaults. These are the ones worth knowing about:

| Variable | Effect when unset |
| --- | --- |
| `EXPLORER_FIRMS_MAP_KEY` | The wildfire layer degrades (FIRMS rejects the bundled `DEMO_KEY` with a 401). |
| `EXPLORER_AIS_API_KEY` | The vessel layer degrades; the AISStream WebSocket never completes its handshake. |
| `PORT` | Listens on 8080. |
| `EIE_JAVA_OPTS` | `-Xmx1g`. |
| `EIE_SKIP_TESTS` | `1` skips the test suite during `run`. |
| `EIE_SKIP_CESIUM` | `1` skips vendoring Cesium and relies on the CDN. |
| `EIE_OPEN` | `0` stops `run` from opening a browser. |
| `FOREGROUND` | `1` keeps `run` in the foreground instead of detaching, for systemd, Docker or CI. |

`GET /api/v1/system/configuration` reports the effective configuration with secrets redacted,
including whether each upstream is still on a demo tier.

## The dashboard

Served from `src/main/resources/static/` at `/`. It needs no build step and no Cesium ion token.

The basemap is the Natural Earth II tile set that ships inside Cesium, served from the local
vendored copy: real coastlines, no token, and no outbound request. It is dimmed and desaturated
so the data layers stay the loudest thing on screen, and the globe colour underneath it is dark
enough that missing tiles blend in rather than flashing.
the viewer runs on an ellipsoid with a local backdrop so the deployment has no external
account dependency.

- **Layers** are defined once, server-side, in `SystemController#/layers`. The dashboard reads
  that catalogue rather than duplicating it, so adding a layer is a backend change.
- **Degradation is first-class.** A layer whose upstream is unavailable shows a banner with the
  reason instead of silently rendering nothing. `meta.degraded` is the client-facing truth, and
  `meta.details.featureCount` always equals the length of `features`.
- **Telemetry** arrives on `/ws/telemetry` as `hello` then periodic `tick` frames, each carrying
  per-domain counts and degraded flags. The client reconnects with capped exponential backoff and
  re-subscribes, because a new socket has no history.
- **Dense lattices** (wind, air quality, currents, SST) render through a single batched
  `PointPrimitiveCollection`; discrete features (quakes, volcanoes, fires, aircraft, vessels,
  tide stations) render as pickable entities.
- **Vegetation** cannot be sampled as vectors — the GIBS layer is not queryable — so the API
  returns a WMS descriptor (`renderAs: "imagery"`) and the dashboard adds it as a
  `WebMapServiceImageryProvider`.

### Offline Cesium

The dashboard prefers a vendored copy of Cesium and falls back to a CDN when none is
present. To run with no outbound network:

```bash
./scripts/eie.sh fetch-cesium
```

The build does this for you automatically when the copy is missing, so a fresh checkout only needs
`./scripts/run.sh` and network access to the npm registry. Set `EIE_SKIP_CESIUM=1` to skip it.

This writes ~12 MB to `src/main/resources/static/cesium/` (gitignored). Rebuild the jar to serve
it. The version is pinned by `CESIUM_VERSION` in the script.

## API

All responses are GeoJSON `FeatureCollection`s. `meta` is authoritative:

```json
{
  "type": "FeatureCollection",
  "features": [],
  "meta": {
    "sources": ["USGS"],
    "degraded": true,
    "error": "daily quota exhausted",
    "details": { "featureCount": 0, "chunksRequested": 4, "chunksFailed": 4 }
  }
}
```

A degraded layer is still `200`; `degraded` lives in the body, not the status code. Parameter
out of range is a `400` with a message naming the parameter and its range — values are rejected
rather than clamped, so a client never silently receives a different lattice than it asked for.

### Endpoints

```bash
# Tectonics
GET /api/v1/tectonics                     composite: earthquakes + volcanoes
GET /api/v1/tectonics/earthquakes?minMagnitude=4.5&maxResults=2000&bbox=-180,-60,180,60
GET /api/v1/tectonics/volcanoes?days=90
GET /api/v1/tectonics/viewport?bbox=...   the same, framed for a map viewport

# Atmospherics
GET /api/v1/atmospherics/wind?gridStep=15
GET /api/v1/atmospherics/air-quality?gridStep=5
GET /api/v1/atmospherics/inspect?lat=51.5&lon=-0.13   point readout for the HUD

# Oceans
GET /api/v1/oceans/tides?days=2
GET /api/v1/oceans/currents?gridStep=15
GET /api/v1/oceans/sea-surface-temperature?gridStep=15
GET /api/v1/oceans/water-temperature
GET /api/v1/oceans/stations
GET /api/v1/oceans/stations/{id}

# Biosphere
GET /api/v1/biosphere/wildfires?days=1&minConfidence=3
GET /api/v1/biosphere/vegetation
GET /api/v1/biosphere/vegetation/layers
GET /api/v1/biosphere/providers

# Human impact
GET /api/v1/human-impact/flights?maxResults=8000&airborneOnly=true
GET /api/v1/human-impact/vessels?maxResults=5000
GET /api/v1/human-impact/ais/status

# Per-domain rollups
GET /api/v1/{domain}/summary

# System
GET /api/v1/system/info
GET /api/v1/system/layers                 the layer catalogue the dashboard reads
GET /api/v1/system/configuration
GET /api/v1/system/caches

# Telemetry
WS  /ws/telemetry                         hello, tick, pong, error frames
GET /api/v1/telemetry/stream              the same ticks as SSE
GET /api/v1/telemetry/latest
GET /api/v1/telemetry/status
```

`gridStep` is in degrees. `bbox` is `west,south,east,north` and is applied as a post-cache
viewport filter, so panning does not cause a new upstream fetch.

### Telemetry frames

Server to client:

```json
{"type":"hello","serverTime":1735689600000,"tickIntervalMs":10000,"apiVersion":"1.0.0","layers":[...]}
{"type":"tick","seq":42,"serverTime":...,"clients":1,"summaries":{...},"latest":[...]}
{"type":"pong","clientTime":...,"serverTime":...}
{"type":"error","code":"BAD_MESSAGE","message":"..."}
```

Client to server:

```json
{"type":"ping","clientTime":1735689600000}
{"type":"subscribe","layers":["tectonics","biosphere"]}
```

A socket is not replayed: a missed tick is stale data, and stale data drawn as current is worse
than a gap. Reconnecting clients start from the next tick.

## Tests

```bash
./scripts/eie.sh build     # mvnw package: compiles and runs the suite
```

The suite covers upstream client parsing and failure handling (MockWebServer fixtures), the
degradation contract, retry classification, tide interpolation, controller parameter validation,
WebSocket session protocol, and the `featureCount` invariant.

`FeatureCountInvariantTest` is worth knowing about: `meta.details.featureCount` is derived in
`GeoJsonPayload`'s canonical constructor precisely so that no ordering of
`withFeatures`/`withMeta` can desynchronise it. That class of bug shipped four times before the
invariant was pushed into the type.

## Layout

```
scripts/
  eie.sh                 process control: run, start, stop, restart, status, logs, clean
  run.sh                 build-if-needed, then start
src/main/java/com/earthinformatics/explorer/
  client/                upstream HTTP clients (USGS, EONET, NOAA, Open-Meteo, FIRMS, WMS, AIS)
  controller/            REST endpoints, payload merging, layer catalogue
  service/               per-domain assembly, caching, degradation decisions
  realtime/              telemetry broadcaster, WebSocket handler
  dto/                   GeoJsonPayload, Meta, DomainSummary, query records
  util/                  GeoJson, QueryParameters, UpstreamRetry, cache support
src/main/resources/
  application.yml        ports, upstreams, resilience, telemetry
  landmask-1deg.bin      marine land mask for Open-Meteo cell selection
  static/                the dashboard (hand-written; cesium/ is vendored)
src/test/java/...        client, controller, dto, realtime, service, util
```
