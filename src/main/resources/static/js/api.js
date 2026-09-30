/**
 * Server-side catalogue and fetch helpers.
 *
 * Everything about a layer - its endpoint, the feature `type` to filter on, its colour, its
 * refresh cadence - is defined once in SystemController#/layers. The dashboard reads that rather
 * than duplicating the list, so a new layer or a renamed property is a backend change only.
 */

const jsonHeaders = { Accept: 'application/json' };

export class ApiError extends Error {
  constructor(message, { status, url } = {}) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.url = url;
  }
}

/**
 * The API distinguishes "this layer is broken" from "this layer is empty" in the body, not the
 * status code: a degraded layer is still a 200. Callers must read meta.degraded, so this throws
 * only for transport and protocol failures.
 */
async function getJson(url, { signal } = {}) {
  let response;
  try {
    response = await fetch(url, { headers: jsonHeaders, signal });
  } catch (cause) {
    if (cause.name === 'AbortError') throw cause;
    throw new ApiError(`network error: ${cause.message}`, { url });
  }
  if (!response.ok) {
    throw new ApiError(`HTTP ${response.status} ${response.statusText}`, { status: response.status, url });
  }
  try {
    return await response.json();
  } catch (cause) {
    throw new ApiError(`malformed JSON: ${cause.message}`, { status: response.status, url });
  }
}

/** The layer catalogue plus the telemetry endpoints, straight from the server. */
export async function loadCatalogue() {
  return getJson('/api/v1/system/layers');
}

export async function loadProviders() {
  return getJson('/api/v1/biosphere/providers');
}

export async function loadSummary(domain) {
  return getJson(`/api/v1/${domain}/summary`);
}

/**
 * Builds a request URL for a layer.
 *
 * `params` are the parameter names the catalogue advertises; unknown ones are dropped rather
 * than sent, because the API rejects out-of-range or unrecognised values with a 400 and a
 * whole-layer failure is a much worse outcome than a missing refinement.
 */
export function buildUrl(layer, params = {}) {
  const url = new URL(layer.endpoint, window.location.origin);
  const allowed = new Set(layer.parameters || []);
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue;
    if (allowed.size && !allowed.has(key)) continue;
    url.searchParams.set(key, String(value));
  }
  return url.pathname + url.search;
}

/**
 * Fetches one layer.
 *
 * Returns a normalised result rather than throwing for degradation, so a single broken upstream
 * degrades one row in the panel instead of tearing down the dashboard.
 */
export async function fetchLayer(layer, params, { signal } = {}) {
  const url = buildUrl(layer, params);
  const started = performance.now();
  try {
    const payload = await getJson(url, { signal });
    const meta = payload.meta || {};
    const features = Array.isArray(payload.features) ? payload.features : [];
    return {
      layer,
      url,
      features,
      meta,
      degraded: Boolean(meta.degraded),
      error: meta.error || null,
      // Some payloads (NDVI) are not vectors at all but a descriptor for an imagery provider.
      renderAs: (meta.details || {}).renderAs || 'vector',
      details: meta.details || {},
      elapsedMs: Math.round(performance.now() - started),
    };
  } catch (error) {
    if (error.name === 'AbortError') throw error;
    return {
      layer,
      url,
      features: [],
      meta: {},
      degraded: true,
      error: error.message,
      renderAs: 'vector',
      details: {},
      elapsedMs: Math.round(performance.now() - started),
      failed: true,
    };
  }
}

/** Camera rectangle -> "west,south,east,north", the bbox form the API accepts. */
export function bboxFromRectangle(rectangle) {
  if (!rectangle) return null;
  const west = Cesium.Math.toDegrees(rectangle.west);
  const south = Cesium.Math.toDegrees(rectangle.south);
  const east = Cesium.Math.toDegrees(rectangle.east);
  const north = Cesium.Math.toDegrees(rectangle.north);
  return { west, south, east, north, param: `${west},${south},${east},${north}` };
}
