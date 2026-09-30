/**
 * Telemetry WebSocket client.
 *
 * The server broadcasts a tick per domain at a fixed cadence; this client keeps a connection,
 * measures round-trip latency, and reconnects with capped exponential backoff.
 *
 * On reconnect the socket is a fresh session with no history, so the client re-requests the
 * layers it cares about rather than assuming a resume: the server deliberately does not replay
 * ticks (a missed tick is stale data, and stale data drawn as current is worse than a gap).
 */

const MAX_BACKOFF_MS = 30_000;
const BASE_BACKOFF_MS = 500;

export class TelemetryClient extends EventTarget {
  constructor(path = '/ws/telemetry') {
    super();
    this.path = path;
    this.socket = null;
    this.state = 'idle';
    this.seq = -1;
    this.latencyMs = null;
    this.attempt = 0;
    this.wantLayers = null;
    this.closedByUs = false;
    this.reconnectTimer = null;
  }

  connect() {
    this.closedByUs = false;
    this._open();
  }

  _setState(state) {
    if (this.state === state) return;
    this.state = state;
    this.dispatchEvent(new CustomEvent('state', { detail: state }));
  }

  _url() {
    const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
    return `${scheme}://${window.location.host}${this.path}`;
  }

  _open() {
    this._setState('connecting');
    let socket;
    try {
      socket = new WebSocket(this._url());
    } catch (error) {
      this._scheduleReconnect();
      return;
    }
    this.socket = socket;

    socket.addEventListener('open', () => {
      this.attempt = 0;
      this._setState('live');
      // Re-assert our filter: a new socket starts with no subscriptions.
      if (this.wantLayers) this._send({ type: 'subscribe', layers: this.wantLayers });
    });

    socket.addEventListener('message', (event) => this._onMessage(event));

    socket.addEventListener('close', () => {
      this.socket = null;
      if (this.closedByUs) {
        this._setState('idle');
        return;
      }
      this._setState('down');
      this._scheduleReconnect();
    });

    socket.addEventListener('error', () => {
      // 'close' always follows; reconnect logic lives there so it runs exactly once.
    });
  }

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    const delay = Math.min(BASE_BACKOFF_MS * 2 ** this.attempt, MAX_BACKOFF_MS);
    this.attempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.closedByUs) this._open();
    }, delay);
  }

  _send(message) {
    if (this.socket && this.socket.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify(message));
      return true;
    }
    return false;
  }

  /** Restricts ticks to the given domains. Pass null for everything. */
  subscribe(layers) {
    this.wantLayers = layers && layers.length ? layers : null;
    this._send({ type: 'subscribe', layers: this.wantLayers || [] });
  }

  /** Measures round-trip time; the server echoes clientTime in its pong. */
  ping() {
    this._send({ type: 'ping', clientTime: Date.now() });
  }

  _onMessage(event) {
    let frame;
    try {
      frame = JSON.parse(event.data);
    } catch {
      return;
    }
    switch (frame.type) {
      case 'hello':
        this.tickIntervalMs = frame.tickIntervalMs;
        this.apiVersion = frame.apiVersion;
        this.dispatchEvent(new CustomEvent('hello', { detail: frame }));
        break;
      case 'tick':
        // A tick older than one we already rendered means an out-of-order redelivery.
        if (typeof frame.seq === 'number' && frame.seq <= this.seq) return;
        this.seq = frame.seq;
        this.dispatchEvent(new CustomEvent('tick', { detail: frame }));
        break;
      case 'pong':
        if (frame.clientTime) this.latencyMs = Date.now() - frame.clientTime;
        break;
      case 'error':
        this.dispatchEvent(new CustomEvent('servererror', { detail: frame }));
        break;
      default:
        break;
    }
  }

  disconnect() {
    this.closedByUs = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.socket) this.socket.close();
    this.socket = null;
    this._setState('idle');
  }
}
