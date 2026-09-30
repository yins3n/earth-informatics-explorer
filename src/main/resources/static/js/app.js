/**
 * Earth Informatics Explorer dashboard.
 *
 * Responsibilities kept here: viewer lifecycle, layer scheduling, camera-driven bbox, telemetry
 * wiring and DOM updates. Data fetching lives in api.js, rendering in layers.js, the socket in
 * telemetry.js.
 */

import { loadCatalogue, loadProviders, fetchLayer, bboxFromRectangle } from './api.js';
import { renderDense, renderEntities, imageryFromDescriptor, legendFor, swatchFor } from './layers.js';
import { TelemetryClient } from './telemetry.js';

const DOMAIN_LABELS = {
  tectonics: 'Tectonics',
  atmospherics: 'Atmosphere',
  oceans: 'Oceans',
  biosphere: 'Biosphere',
  'human-impact': 'Human impact',
};

/** Layers that must not be requested with a bbox until the camera is actually close in. */
const BBOX_MIN_HEIGHT_DEGREES = 25;
const CAMERA_SETTLE_MS = 600;

/**
 * Query parameters the dashboard has an opinion about. Anything not listed here is left to the
 * server default, so the API stays the single source of truth for a layer's defaults and the
 * dashboard does not silently drift from it.
 */
const PARAM_DEFAULTS = {
  // The catalogue legend reads "Magnitude 2.5+", so match it: below that the map is noise.
  earthquakes: { minMagnitude: 2.5 },
  // EONET volcano events are infrequent; a 90-day window is genuinely empty most of the time.
  // The server default of 365 is the useful one, so it is not overridden here.
  tides: { days: 2 },
};

const state = {
  viewer: null,
  catalogue: { layers: [], domains: [], telemetry: {} },
  providers: {},
  /** layerId -> { def, enabled, loading, status, error, count, elapsedMs, source, imagery } */
  layers: new Map(),
  telemetry: null,
  bbox: null,
  bboxTimer: null,
  picked: null,
};

// --------------------------------------------------------------------- boot

const bootEl = document.getElementById('boot');
const bootDetail = document.getElementById('boot-detail');
const bootFailed = document.getElementById('boot-failed');

function loadScript(src) {
  return new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.src = src;
    script.onload = resolve;
    script.onerror = () => reject(new Error(`failed to load ${src}`));
    document.head.appendChild(script);
  });
}

/**
 * Prefers a vendored copy so the dashboard works with no outbound network, then falls back to
 * the CDN. CESIUM_BASE_URL must be set before any Cesium asset (workers, textures) is resolved.
 *
 * Returns the label of the source that worked, or an object describing why none of them did,
 * because "Cesium could not be loaded" on its own is useless to whoever has to fix it. The
 * three common causes need completely different remedies.
 */
async function ensureCesium() {
  if (window.Cesium) return { ok: true, label: 'preloaded' };

  // Opened straight off disk. Every path below is either root-relative (which resolves to the
  // filesystem root under file://) or cross-origin, and the API cannot be reached from a
  // "null" origin either, so no amount of fetching a local copy would make this work.
  if (location.protocol === 'file:') {
    return {
      ok: false,
      reason: 'file-protocol',
      detail: 'This page was opened as a local file. The dashboard needs to be served over HTTP '
        + 'so it can load the API and the Cesium assets from the same origin.',
    };
  }

  const sources = [
    { base: '/cesium', label: 'local copy' },
    { base: 'https://cdn.jsdelivr.net/npm/cesium@1.121.0/Build/Cesium', label: 'CDN' },
  ];
  const attempts = [];
  for (const source of sources) {
    try {
      bootDetail.textContent = `loading Cesium (${source.label})`;
      window.CESIUM_BASE_URL = `${source.base}/`;
      await loadScript(`${source.base}/Cesium.js`);
      await ensureCesiumWidgets();
      return { ok: true, label: source.label, attempts };
    } catch (error) {
      console.warn('Cesium source failed:', source.label, error.message);
      attempts.push(`${source.label}: ${error.message}`);
    }
  }
  return {
    ok: false,
    reason: 'unavailable',
    attempts,
    detail: 'Neither the vendored copy nor the CDN could be reached.',
  };
}

/**
 * The single place that decides what the failure panel says. Every failure path goes through it,
 * because a panel that blames the wrong thing costs more time than no panel at all: a generic
 * "Cesium could not be loaded" is exactly what this replaces.
 */
function showBootFailure({ heading, body, command = '', detail = '' }) {
  document.getElementById('boot-failed-heading').textContent = heading;
  document.getElementById('boot-failed-body').textContent = body;
  const commandEl = document.getElementById('boot-failed-command');
  commandEl.textContent = command;
  commandEl.hidden = !command;
  const detailEl = document.getElementById('boot-failed-detail');
  detailEl.textContent = detail;
  detailEl.hidden = !detail;
  bootEl.hidden = true;
  bootFailed.hidden = false;
}

/** True when the browser can give Cesium a WebGL 2 context at all. */
function webglProblem() {
  try {
    const canvas = document.createElement('canvas');
    if (!canvas.getContext('webgl2')) {
      return 'This browser did not provide a WebGL 2 context, which Cesium requires. That '
        + 'usually means hardware acceleration is switched off, or the page is running in an '
        + 'environment without GPU support.';
    }
    return null;
  } catch (error) {
    return `WebGL 2 could not be initialised: ${error.message}`;
  }
}

/** Turns a loader failure into text that names the thing to go and fix. */
function describeBootFailure(failure) {
  if (failure.reason === 'file-protocol') {
    return {
      heading: 'Open the dashboard through the server',
      body: 'This page was opened as a local file, so it cannot reach the API or the Cesium '
        + 'assets. Start the server and use the URL it prints.',
      command: location.origin === 'null' ? './scripts/run.sh' : '',
    };
  }
  const localMissing = failure.attempts?.some((a) => a.startsWith('local copy'));
  return {
    heading: localMissing
      ? 'No local copy of Cesium, and the CDN was unreachable'
      : 'Cesium could not be loaded',
    body: localMissing
      ? 'The vendored copy is missing and the CDN did not respond, which usually means the '
        + 'network blocks it. Either fetch the local copy, or allow cdn.jsdelivr.net.'
      : 'The CDN did not respond. Check the network, or fetch a local copy to run offline.',
    command: './scripts/eie.sh fetch-cesium',
  };
}

function loadStylesheet(href) {
  return new Promise((resolve, reject) => {
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = href;
    link.onload = resolve;
    link.onerror = () => reject(new Error(`failed to load ${href}`));
    document.head.appendChild(link);
  });
}

async function ensureCesiumWidgets() {
  if (!window.Cesium) return;
  // Must be a <link>, not a <script>: injected as script the browser parses the CSS as
  // JavaScript and throws "Unexpected token '.'" on the leading selector, and the rule that
  // sizes the canvas to its container never applies.
  await loadStylesheet(`${window.CESIUM_BASE_URL}Widgets/widgets.css`).catch(() => {});
}

async function start() {
  const source = await ensureCesium();
  if (!source.ok) {
    const described = describeBootFailure(source);
    showBootFailure({ ...described, detail: source.detail || '' });
    return;
  }

  bootDetail.textContent = 'creating viewer';
  await createViewer();

  bootDetail.textContent = 'loading layer catalogue';
  try {
    const catalogue = await loadCatalogue();
    state.catalogue = catalogue;
    state.layers = new Map(
      catalogue.layers.map((layer) => [layer.id, {
        def: layer, enabled: false, loading: false, status: 'idle',
        error: null, count: 0, elapsedMs: null, source: null, imagery: null,
      }]),
    );
  } catch (error) {
    setStatus(`layer catalogue unavailable: ${error.message}`);
    bootEl.classList.add('done');
    return;
  }

  buildLayerList();
  buildLegend(null);
  buildDomainStrip();
  await loadProvidersPanel();

  // Default view: Earth at a mid altitude, which shows all five domains at once.
  state.viewer.camera.flyTo({
    destination: Cesium.Cartesian3.fromDegrees(8, 18, 16_000_000),
    duration: 0,
  });

  startTelemetry();
  wireCamera();
  wirePicking();
  wirePanelToggle();

  // A handful of layers on by default; the heavy lattices stay off until asked for.
  const DEFAULT_ON = ['earthquakes', 'volcanoes', 'tides', 'flights'];
  for (const id of DEFAULT_ON) {
    setLayerEnabled(id, true);
  }

  // Exposed deliberately: it is the handle for debugging from the browser console
  // (viewer.entities, viewer.dataSources, viewer.scene) and for the smoke test.
  window.__eie = state;

  bootEl.classList.add('done');
  setTimeout(() => { bootEl.style.display = 'none'; }, 500);
  setStatus('ready');
}

// ------------------------------------------------------------------- viewer

async function createViewer() {
  // No Cesium ion token: this deployment must work with no external account, so the default
  // ion world imagery and terrain are switched off and replaced with a plain ellipsoid.
  Cesium.Ion.defaultAccessToken = undefined;

  state.viewer = new Cesium.Viewer('globe', {
    baseLayerPicker: false,
    baseLayer: false,
    terrainProvider: new Cesium.EllipsoidTerrainProvider(),
    geocoder: false,
    homeButton: false,
    sceneModePicker: false,
    navigationHelpButton: false,
    animation: false,
    timeline: false,
    fullscreenButton: false,
    infoBox: false,
    selectionIndicator: false,
    requestRenderMode: false,
  });

  state.viewer.scene.globe.baseColor = Cesium.Color.fromCssColorString('#0a1020');
  state.viewer.scene.globe.showGroundAtmosphere = true;
  state.viewer.scene.globe.atmosphereBrightnessShift = 0.25;
  state.viewer.scene.backgroundColor = Cesium.Color.fromCssColorString('#05070d');
  state.viewer.scene.globe.enableLighting = true;
  state.viewer.scene.globe.depthTestAgainstTerrain = false;
  state.viewer.cesiumWidget.creditContainer.style.display = 'none';

  // Basemap: the Natural Earth II tile set that ships inside Cesium itself. It is real
  // cartography, it is served from the local vendored copy so the dashboard still works with no
  // network and no external account, and it needs no Cesium ion token. The globe's base colour
  // stays dark underneath so the data layers above it remain the loudest thing on screen.
  const basemap = await Cesium.TileMapServiceImageryProvider.fromUrl(
    Cesium.buildModuleUrl('Assets/Textures/NaturalEarthII'),
  );
  const basemapLayer = state.viewer.imageryLayers.addImageryProvider(basemap);
  basemapLayer.brightness = 0.72;
  basemapLayer.saturation = 0.35;
  basemapLayer.contrast = 1.05;

  // Cesium caches its drawing-buffer size and does not re-measure the container on its own,
  // so the first paint uses the canvas default of 300x150 until resize() is called.
  const syncSize = () => {
    if (state.viewer && !state.viewer.isDestroyed()) {
      state.viewer.resize();
      state.viewer.scene.requestRender();
    }
  };
  window.addEventListener('resize', syncSize);
  new ResizeObserver(syncSize).observe(document.getElementById('globe'));
  // A frame later, so the container has been laid out.
  requestAnimationFrame(syncSize);
}

/**
 * A dark, low-contrast land backdrop drawn as an inline SVG data URI. Avoids pulling a tile
 * service (and its attribution obligations) for what is only context behind the data.
 */
// -------------------------------------------------------------------- layers

function layerState(id) {
  return state.layers.get(id);
}

function setLayerEnabled(id, enabled, { silent = false } = {}) {
  const entry = layerState(id);
  if (!entry || entry.enabled === enabled) return;
  entry.enabled = enabled;
  updateLayerRow(entry);
  buildLegend(enabled ? entry : null);
  if (enabled) {
    refreshLayer(id);
  } else {
    teardown(entry);
    setStatus(`${entry.def.label} hidden`);
  }
  if (!silent) syncSubscription();
}

function teardown(entry) {
  if (entry.entitySource) {
    state.viewer.dataSources.remove(entry.entitySource);
    entry.entitySource = null;
  }
  if (entry.primitive) {
    state.viewer.scene.primitives.remove(entry.primitive);
    entry.primitive = null;
  }
  if (entry.imageryIndex !== undefined && entry.imageryIndex !== null) {
    state.viewer.imageryLayers.remove(entry.imageryIndex, true);
    entry.imageryIndex = null;
  }
  entry.count = 0;
  entry.index = null;
}

async function refreshLayer(id) {
  const entry = layerState(id);
  if (!entry || !entry.enabled) return;
  if (entry.inFlight) entry.abort.abort();
  const abort = new AbortController();
  entry.inFlight = true;
  entry.abort = abort;
  entry.loading = true;
  updateLayerRow(entry);

  const params = { ...(PARAM_DEFAULTS[id] || {}) };
  if (state.bbox) params.bbox = state.bbox.param;

  try {
    const result = await fetchLayer(entry.def, params, { signal: abort.signal });
    if (abort.signal.aborted) return;
    await applyResult(entry, result);
  } catch (error) {
    if (error.name !== 'AbortError') {
      entry.status = 'error';
      entry.error = error.message;
      updateLayerRow(entry);
    }
  } finally {
    entry.inFlight = false;
    entry.loading = false;
    updateLayerRow(entry);
  }
}

async function applyResult(entry, result) {
  teardown(entry);
  const { def } = entry;
  entry.error = result.error;
  entry.elapsedMs = result.elapsedMs;
  entry.details = result.details;
  entry.degraded = result.degraded;

  if (result.renderAs === 'imagery') {
    const imagery = imageryFromDescriptor(def, result.details);
    if (imagery) {
      const layer = state.viewer.imageryLayers.addImageryProvider(imagery.provider);
      entry.imageryIndex = layer;
      entry.count = 0;
      entry.status = result.degraded ? 'imagery' : 'live';
      updateLayerRow(entry);
      return;
    }
  }

  // The catalogue names the feature type to filter on; a mismatch is a server bug and is
  // reported as such rather than silently drawing an empty layer.
  const wanted = def.featureType;
  const features = wanted
    ? result.features.filter((f) => f.properties && f.properties.type === wanted)
    : result.features;

  if (features.length === 0 && result.features.length > 0) {
    const seen = [...new Set(result.features
      .map((f) => (f.properties || {}).type).filter(Boolean))];
    entry.status = 'mismatch';
    entry.error = `expected featureType "${wanted}", received [${seen.join(', ')}]`;
    entry.count = 0;
    updateLayerRow(entry);
    return;
  }

  if (isDense(def)) {
    const dense = renderDense(def, features, result.meta);
    entry.primitive = state.viewer.scene.primitives.add(dense.primitive);
    entry.index = dense.index;
    entry.count = dense.count;
  } else {
    entry.entitySource = await state.viewer.dataSources.add(
      renderEntities(def, features),
    );
    entry.count = features.length;
  }

  entry.status = result.degraded ? 'degraded' : 'live';
  updateLayerRow(entry);
}

const isDense = (def) => ['wind', 'air-quality', 'currents', 'sea-surface-temperature'].includes(def.id);

/** Each layer refreshes on the cadence the server advertises for it. */
function startRefreshLoop() {
  for (const entry of state.layers.values()) {
    const every = Math.max(15_000, entry.def.refreshMs || 60_000);
    setInterval(() => {
      // Only refresh what is on screen and in view, or a hidden layer would still be fetched.
      if (entry.enabled && !document.hidden) refreshLayer(entry.def.id);
    }, every);
  }
}

// ------------------------------------------------------------------ camera

function wireCamera() {
  const handler = () => {
    clearTimeout(state.bboxTimer);
    state.bboxTimer = setTimeout(updateBbox, CAMERA_SETTLE_MS);
  };
  state.viewer.camera.changed.addEventListener(handler);
  state.viewer.camera.moveEnd.addEventListener(handler);
}

function updateBbox() {
  const rectangle = state.viewer.camera.viewRectangle;
  const bbox = bboxFromRectangle(rectangle);
  const height = rectangle ? Cesium.Math.toDegrees(rectangle.north - rectangle.south) : 180;
  // Zoomed out, the whole planet is the view; asking for a bbox that happens to be the whole
  // planet just adds a redundant post-cache filter.
  const next = bbox && height < BBOX_MIN_HEIGHT_DEGREES ? bbox : null;
  const changed = (next?.param || null) !== (state.bbox?.param || null);
  state.bbox = next;
  if (changed) {
    for (const entry of state.layers.values()) {
      if (entry.enabled) refreshLayer(entry.def.id);
    }
  }
  updateCameraReadout(bbox, height);
}

// ------------------------------------------------------------------ picking

function wirePicking() {
  const handler = new Cesium.ScreenSpaceEventHandler(state.viewer.scene.canvas);
  handler.setInputAction((movement) => {
    const picked = state.viewer.scene.pick(movement.position);
    const feature = resolvePicked(picked);
    if (!feature) {
      state.picked = null;
      renderInspect(null);
      return;
    }
    state.picked = feature;
    renderInspect(feature);
  }, Cesium.ScreenSpaceEventType.LEFT_CLICK);
}

function resolvePicked(picked) {
  if (!picked) return null;
  // Entities carry the properties bag set in renderEntities.
  const entity = picked.id instanceof Cesium.Entity ? picked.id : null;
  if (entity?.properties) {
    const values = entity.properties.getValue();
    return { layerId: values.layerId, properties: values.meta, title: values.title };
  }
  // Primitives carry the feature directly in their id.
  if (picked.id && picked.id.feature) {
    const properties = picked.id.feature.properties || {};
    return {
      layerId: picked.id.layerId,
      properties,
      title: properties.name || properties.title || picked.id.feature.id,
    };
  }
  return null;
}

// ----------------------------------------------------------------- telemetry

function startTelemetry() {
  const telemetry = new TelemetryClient(state.catalogue.telemetry?.websocket || '/ws/telemetry');
  state.telemetry = telemetry;
  const conn = document.getElementById('conn');
  const label = conn.querySelector('.conn-label');

  telemetry.addEventListener('state', (event) => {
    conn.className = `conn ${event.detail}`;
    label.textContent = { live: 'live', connecting: 'connecting', down: 'reconnecting', idle: 'off' }[event.detail] || event.detail;
  });
  telemetry.addEventListener('tick', (event) => renderTick(event.detail));
  telemetry.addEventListener('hello', (event) => {
    setStatus(`connected - api ${event.detail.apiVersion}, ticking every ${Math.round(event.detail.tickIntervalMs / 1000)}s`);
  });
  telemetry.addEventListener('servererror', (event) => {
    setStatus(`server rejected a frame: ${event.detail.message}`);
  });

  telemetry.connect();
  setInterval(() => telemetry.ping(), 20_000);
  syncSubscription();
  startRefreshLoop();
}

/** Only subscribe to domains with a visible layer, to keep ticks lean. */
function syncSubscription() {
  if (!state.telemetry) return;
  const domains = [...new Set(
    [...state.layers.values()].filter((e) => e.enabled).map((e) => e.def.domain),
  )];
  state.telemetry.subscribe(domains);
}

function renderTick(tick) {
  const strip = document.getElementById('domain-strip');
  const summaries = tick.summaries || {};
  strip.innerHTML = '';

  for (const [domain, summary] of Object.entries(summaries)) {
    const chip = document.createElement('div');
    chip.className = `domain-chip${summary.degraded ? ' degraded' : ''}`;
    const count = Number(summary.count ?? 0);
    chip.title = summary.degraded
      ? `${DOMAIN_LABELS[domain] || domain}: degraded - ${summary.metrics?.reason || 'upstream unavailable'}`
      : `${DOMAIN_LABELS[domain] || domain}: ${count} features`;
    chip.innerHTML = `<span>${escapeHtml(DOMAIN_LABELS[domain] || domain)}</span>`
      + `<span class="n">${formatCount(count)}</span>`;
    strip.appendChild(chip);
  }

  const latency = state.telemetry?.latencyMs;
  const parts = [`tick #${tick.seq}`, `${formatCount(Object.keys(summaries).length)} domains`];
  if (latency !== null && latency !== undefined) parts.push(`${latency}ms rtt`);
  setStatus(parts.join('  -  '));
}

// ----------------------------------------------------------------------- UI

function buildLayerList() {
  const list = document.getElementById('layer-list');
  list.innerHTML = '';
  for (const entry of state.layers.values()) {
    const row = document.createElement('div');
    row.className = 'layer-row';
    row.dataset.id = entry.def.id;
    row.innerHTML = `
      <div class="layer-main">
        <div class="layer-name">
          <span class="swatch" style="background:${swatchFor(entry.def)}"></span>
          <span>${escapeHtml(entry.def.label)}</span>
        </div>
        <div class="layer-sub"></div>
      </div>
      <button class="toggle" role="switch" aria-checked="false" aria-label="Toggle ${escapeHtml(entry.def.label)}"></button>`;
    row.addEventListener('click', (event) => {
      if (event.target.closest('.toggle')) return;
      setLayerEnabled(entry.def.id, !entry.enabled);
    });
    row.querySelector('.toggle').addEventListener('click', (event) => {
      event.stopPropagation();
      setLayerEnabled(entry.def.id, !entry.enabled);
    });
    list.appendChild(row);
    updateLayerRow(entry);
  }
}

function updateLayerRow(entry) {
  const row = document.querySelector(`.layer-row[data-id="${entry.def.id}"]`);
  if (!row) return;
  row.classList.toggle('active', entry.enabled);
  row.classList.toggle('disabled', !entry.enabled);
  row.querySelector('.toggle').setAttribute('aria-checked', String(entry.enabled));

  const sub = row.querySelector('.layer-sub');
  sub.className = 'layer-sub';
  if (entry.loading) {
    sub.classList.add('loading');
    sub.textContent = 'loading…';
  } else if (!entry.enabled) {
    sub.textContent = `${entry.def.source} · off`;
  } else if (entry.status === 'mismatch') {
    sub.classList.add('degraded');
    sub.textContent = entry.error;
  } else if (entry.degraded) {
    sub.classList.add('degraded');
    sub.textContent = entry.error
      ? `degraded · ${truncate(entry.error, 40)}`
      : 'degraded · upstream unavailable';
  } else if (entry.status === 'imagery') {
    sub.textContent = `${entry.def.source} · imagery${entry.details?.time ? ` @ ${entry.details.time}` : ''}`;
  } else {
    sub.textContent = `${formatCount(entry.count)} features · ${entry.elapsedMs ?? '?'}ms`;
  }

  if (entry.enabled) buildLegend(entry);
  renderAlerts();
}

function buildLegend(entry) {
  const container = document.getElementById('legend');
  container.innerHTML = '';
  if (!entry) {
    container.innerHTML = '<p class="muted">Select a layer to see its scale.</p>';
    return;
  }
  const def = legendFor(entry.def.id);
  if (!def) {
    container.innerHTML = `<p class="muted">${escapeHtml(entry.def.legend || 'No scale defined.')}</p>`;
    return;
  }
  const heading = document.createElement('div');
  heading.className = 'legend-label';
  heading.textContent = `${entry.def.legend}${def.unit ? ` (${def.unit})` : ''}`;
  container.appendChild(heading);

  if (def.type === 'ramp' || def.type === 'magnitude') {
    const gradient = swatchFor({ id: entry.def.id });
    const scale = document.createElement('div');
    scale.className = 'legend-scale';
    scale.style.background = gradient;
    container.appendChild(scale);

    const ticks = document.createElement('div');
    ticks.className = 'legend-ticks';
    ticks.innerHTML = def.ticks.map((t) => `<span>${t}</span>`).join('');
    container.appendChild(ticks);
  } else {
    const note = document.createElement('p');
    note.className = 'legend-note';
    note.textContent = def.text;
    container.appendChild(note);
  }
}

function renderInspect(feature) {
  const container = document.getElementById('inspect');
  if (!feature) {
    container.innerHTML = '<p class="muted">Click a feature on the globe for its properties.</p>';
    return;
  }
  const properties = feature.properties || {};
  const rows = Object.entries(properties)
    .filter(([, v]) => v !== null && v !== undefined && typeof v !== 'object')
    .slice(0, 14)
    .map(([k, v]) => `<dt>${escapeHtml(k)}</dt><dd>${escapeHtml(formatValue(v))}</dd>`)
    .join('');
  const link = properties.eventLink || properties.sourceUrl;
  container.innerHTML = `<div class="title">${escapeHtml(feature.title || 'Feature')}</div>`
    + `<dl>${rows}</dl>`
    + (link ? `<p class="legend-note"><a href="${escapeHtml(link)}" target="_blank" rel="noopener">open source</a></p>` : '');
}

function buildDomainStrip() {
  const strip = document.getElementById('domain-strip');
  strip.innerHTML = '';
  for (const domain of state.catalogue.domains || []) {
    const chip = document.createElement('div');
    chip.className = 'domain-chip';
    chip.innerHTML = `<span>${escapeHtml(DOMAIN_LABELS[domain] || domain)}</span><span class="n">–</span>`;
    strip.appendChild(chip);
  }
}

async function loadProvidersPanel() {
  try {
    state.providers = await loadProviders();
  } catch {
    return;
  }
  const container = document.getElementById('providers');
  const rows = Object.entries(state.providers).map(([key, value]) => {
    const name = value.name || key;
    const note = value.note ? ` - ${value.note}` : '';
    return `<dt>${escapeHtml(name)}</dt><dd>${escapeHtml(value.url || '')}${escapeHtml(note)}</dd>`;
  });
  container.innerHTML = `<dl>${rows.join('')}</dl>`;
}

function renderAlerts() {
  const container = document.getElementById('alerts');
  const broken = [...state.layers.values()]
    .filter((entry) => entry.enabled && (entry.degraded || entry.status === 'mismatch'));
  container.innerHTML = broken.map((entry) => `
    <div class="alert${entry.status === 'mismatch' ? ' bad' : ''}">
      <div class="alert-body">
        <strong>${escapeHtml(entry.def.label)}</strong>
        <span>${escapeHtml(entry.error || 'upstream unavailable')}</span>
      </div>
    </div>`).join('');
}

function wirePanelToggle() {
  const button = document.getElementById('panel-toggle');
  button.addEventListener('click', () => {
    document.body.classList.toggle('panel-hidden');
    // The canvas is the same width regardless; Cesium needs an explicit nudge on layout change.
    setTimeout(() => state.viewer.resize(), 240);
  });
}

// ---------------------------------------------------------------- utilities

function setStatus(text) {
  document.getElementById('status-text').textContent = text;
}

function updateCameraReadout(bbox, height) {
  const readout = document.getElementById('camera-readout');
  if (!bbox) {
    readout.textContent = `global view (${Math.round(height)}° tall)`;
    return;
  }
  readout.textContent = `bbox ${bbox.west.toFixed(1)},${bbox.south.toFixed(1)},`
    + `${bbox.east.toFixed(1)},${bbox.north.toFixed(1)}`;
}

function formatValue(value) {
  if (typeof value === 'number') {
    return Number.isInteger(value) ? String(value) : value.toFixed(3);
  }
  return String(value);
}

function formatCount(n) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

function truncate(text, max) {
  const value = String(text);
  return value.length > max ? `${value.slice(0, max - 1)}…` : value;
}

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

// Cursor readout: cheap and genuinely useful when reading a dense lattice.
document.getElementById('globe')?.addEventListener('mousemove', (event) => {
  if (!state.viewer) return;
  const cartesian = state.viewer.camera.pickEllipsoid(
    new Cesium.Cartesian2(
      event.clientX - state.viewer.scene.canvas.getBoundingClientRect().left,
      event.clientY - state.viewer.scene.canvas.getBoundingClientRect().top,
    ),
    state.viewer.scene.globe.ellipsoid,
  );
  const readout = document.getElementById('cursor-readout');
  if (!cartesian) {
    readout.textContent = '';
    return;
  }
  const carto = Cesium.Cartographic.fromCartesian(cartesian);
  readout.textContent = `${Cesium.Math.toDegrees(carto.latitude).toFixed(2)}, `
    + `${Cesium.Math.toDegrees(carto.longitude).toFixed(2)}`;
});

start().catch((error) => {
  console.error('dashboard failed to start', error);
  const noWebgl = window.Cesium ? webglProblem() : null;
  if (noWebgl) {
    showBootFailure({
      heading: 'This browser cannot render the globe',
      body: 'CesiumJS loaded, but no WebGL 2 context was available, so the map cannot be drawn.',
      detail: noWebgl,
    });
    return;
  }
  showBootFailure({
    heading: 'The dashboard failed to start',
    body: 'This is not a Cesium loading problem: the library loaded and something after it '
      + 'failed. The message below is the actual error.',
    detail: `${error.name}: ${error.message}`,
  });
});
