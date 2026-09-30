#!/usr/bin/env bash
#
# Earth Informatics Explorer - process control.
#
#   ./scripts/eie.sh run              build if needed, start, wait until healthy
#   ./scripts/eie.sh start            start only (no build)
#   ./scripts/eie.sh stop             graceful shutdown
#   ./scripts/eie.sh restart          stop then run
#   ./scripts/eie.sh status           is it up, on which port, since when
#   ./scripts/eie.sh logs [-f]        tail the log
#   ./scripts/eie.sh build            compile and test
#   ./scripts/eie.sh fetch-cesium     vendor Cesium for offline use
#   ./scripts/eie.sh clean            remove build output and the pid file
#
# Environment:
#   PORT              listen port                 (default 8080)
#   JAVA_HOME         JDK to build/run with       (auto-detected: needs 17+)
#   EIE_JAVA_OPTS     extra JVM flags            (default: -Xmx1g)
#   EIE_SKIP_TESTS    set to 1 to skip tests during `run`
#   EIE_SKIP_CESIUM   set to 1 to skip vendoring Cesium and rely on the CDN
#   EIE_OPEN          set to 0 to skip opening a browser
#   FOREGROUND=1      `run` stays in the foreground (for systemd, Docker, CI)
#   EXPLORER_FIRMS_MAP_KEY  NASA FIRMS key - without it the wildfire layer is degraded
#   EXPLORER_AIS_API_KEY    AISStream key  - without it the vessel layer is degraded
#
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
JAR="${PROJECT_DIR}/target/earth-informatics-explorer.jar"
RUN_DIR="${PROJECT_DIR}/.run"
PID_FILE="${RUN_DIR}/app.pid"
LOG_FILE="${RUN_DIR}/app.log"
PORT="${PORT:-8080}"
JAVA_OPTS="${EIE_JAVA_OPTS:--Xmx1g}"
CESIUM_VERSION="${CESIUM_VERSION:-1.121.0}"

mkdir -p "${RUN_DIR}"

# ------------------------------------------------------------------ output

if [ -t 1 ]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GREEN=$'\033[32m'
  YELLOW=$'\033[33m'; BLUE=$'\033[34m'; RESET=$'\033[0m'
else
  BOLD=''; DIM=''; RED=''; GREEN=''; YELLOW=''; BLUE=''; RESET=''
fi

info()  { printf '%s==>%s %s\n' "${BLUE}${BOLD}" "${RESET}" "$*"; }
ok()    { printf '  %s%s%s\n' "${GREEN}" "$*" "${RESET}"; }
warn()  { printf '  %s%s%s\n' "${YELLOW}" "$*" "${RESET}"; }
fail()  { printf '  %s%s%s\n' "${RED}" "$*" "${RESET}" >&2; }
dim()   { printf '  %s%s%s\n' "${DIM}" "$*" "${RESET}"; }

die() { fail "$*"; exit 1; }

# --------------------------------------------------------------- toolchain

# The default `java` on this machine is 26; the project targets 17. Prefer an explicit
# JAVA_HOME, then a 17 install, then whatever is on PATH.
detect_java() {
  if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/javac" ]; then
    return 0
  fi
  local candidate
  for candidate in /usr/lib/jvm/java-17-openjdk /usr/lib/jvm/java-17 \
                   /usr/lib/jvm/default-java-17 /usr/lib/jvm/temurin-17-jdk-amd64; do
    if [ -x "${candidate}/bin/javac" ]; then
      JAVA_HOME="${candidate}"
      export JAVA_HOME
      return 0
    fi
  done
  command -v javac >/dev/null 2>&1 || return 1
  return 0
}

java_major() {
  "${JAVA_HOME}/bin/java" -version 2>&1 | head -1 \
    | sed -E 's/.*version "([0-9]+).*/\1/'
}

# Maven wrapper if present, else a system mvn. The wrapper needs JAVA_HOME set.
maven() {
  if [ -x "${PROJECT_DIR}/mvnw" ]; then
    "${PROJECT_DIR}/mvnw" "$@"
  elif command -v mvn >/dev/null 2>&1; then
    mvn "$@"
  else
    die "no Maven: the wrapper at ${PROJECT_DIR}/mvnw is missing and mvn is not on PATH"
  fi
}

# ------------------------------------------------------------------ status

running_pid() {
  [ -f "${PID_FILE}" ] || return 1
  local pid
  pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  [ -n "${pid}" ] || return 1
  kill -0 "${pid}" 2>/dev/null || return 1
  printf '%s' "${pid}"
}

# A pid file can go stale if the process was killed elsewhere; clear it rather than
# reporting a phantom service.
running_pid >/dev/null || rm -f "${PID_FILE}" 2>/dev/null || true

health_ok() {
  curl -fsS --max-time 3 "http://localhost:${PORT}/api/v1/system/info" >/dev/null 2>&1
}

# ------------------------------------------------------------------ build

build() {
  detect_java || die "no JDK found; set JAVA_HOME"
  local major
  major="$(java_major)"
  if [ -n "${major}" ] && [ "${major}" -lt 17 ] 2>/dev/null; then
    die "JDK ${major} found at ${JAVA_HOME}; Spring Boot 3.3 needs 17 or newer"
  fi
  info "building with JDK ${major} (${JAVA_HOME})"

  # The dashboard cannot boot without CesiumJS. Fetch it here, before packaging, rather than
  # leaving it as a manual step someone discovers through a browser error: a fresh checkout
  # would otherwise build a jar that serves a dashboard which cannot possibly start.
  # EIE_SKIP_CESIUM=1 opts out and leaves the CDN as the only source.
  local cesium_dir="${PROJECT_DIR}/src/main/resources/static/cesium"
  if [ ! -f "${cesium_dir}/Cesium.js" ] && [ "${EIE_SKIP_CESIUM:-0}" != "1" ]; then
    if command -v curl >/dev/null 2>&1 && command -v tar >/dev/null 2>&1; then
      warn "no vendored Cesium; fetching it now"
      cmd_fetch_cesium || warn "could not fetch Cesium; the dashboard will fall back to the CDN"
    else
      warn "no vendored Cesium and curl/tar unavailable; the dashboard will use the CDN"
    fi
  fi

  if [ "${EIE_SKIP_TESTS:-0}" = "1" ]; then
    warn "skipping tests (EIE_SKIP_TESTS=1)"
    maven -B -q -DskipTests package
  else
    maven -B -q package
  fi
  [ -f "${JAR}" ] || die "build finished but ${JAR} is missing"

  # Cheap preflight: say it now, at the terminal, instead of letting a browser discover it.
  # grep -c rather than -q: -q exits on the first match, which SIGPIPEs unzip, and under
  # `set -o pipefail` that non-zero status reads as "not found".
  if [ "$(unzip -l "${JAR}" 2>/dev/null | grep -c 'static/cesium/Cesium.js')" = "0" ]; then
    warn "jar contains no Cesium; the dashboard will need the CDN at runtime"
  fi
  ok "built $(basename "${JAR}") ($(du -h "${JAR}" | cut -f1))"
}

# -------------------------------------------------------------------- run

wait_for_health() {
  local attempts="${1:-90}"
  local i
  for ((i = 1; i <= attempts; i++)); do
    if health_ok; then
      return 0
    fi
    if ! running_pid >/dev/null; then
      fail "the process exited during startup; last lines:"
      tail -n 20 "${LOG_FILE}" >&2
      return 1
    fi
    sleep 1
  done
  return 1
}

cmd_run() {
  if running_pid >/dev/null; then
    local pid
    pid="$(running_pid)"
    warn "already running (pid ${pid}) on port ${PORT}"
    return 0
  fi
  build
  # FOREGROUND=1 runs the JVM in this process instead of detaching it. That is what a service
  # manager or a container wants: the supervisor owns the pid, stdout goes to the journal, and
  # SIGTERM from `systemctl stop` reaches the app directly.
  if [ "${FOREGROUND:-0}" = "1" ]; then
    info "starting in the foreground on port ${PORT} (ctrl-c to stop)"
    exec "${JAVA_HOME}/bin/java" ${JAVA_OPTS} \
      -jar "${JAR}" --server.port="${PORT}"
  fi
  cmd_start
}

cmd_start() {
  [ -f "${JAR}" ] || die "${JAR} not found - run './scripts/eie.sh run' first"
  detect_java || die "no JDK found; set JAVA_HOME"

  local pid
  if running_pid >/dev/null; then
    pid="$(running_pid)"
    info "stopping previous instance (pid ${pid})"
    kill "${pid}" 2>/dev/null || true
    sleep 2
  fi

  # Any process already holding the port, regardless of how it was started.
  if command -v fuser >/dev/null 2>&1 && fuser "${PORT}/tcp" >/dev/null 2>&1; then
    die "port ${PORT} is already in use by another process (fuser ${PORT}/tcp to identify)"
  fi

  info "starting on port ${PORT}"
  : > "${LOG_FILE}"
  # setsid detaches the app into its own session. nohup alone is not enough: the process stays
  # in the launcher's process group, so a shell that exits - or a terminal that is closed -
  # takes the server down with it. setsid is in util-linux; fall back to plain nohup.
  local launcher=()
  command -v setsid >/dev/null 2>&1 && launcher=(setsid)
  "${launcher[@]}" nohup "${JAVA_HOME}/bin/java" ${JAVA_OPTS} \
    -jar "${JAR}" --server.port="${PORT}" >> "${LOG_FILE}" 2>&1 < /dev/null &
  pid=$!
  printf '%s' "${pid}" > "${PID_FILE}"

  if wait_for_health 90; then
    ok "up in ${SECONDS}s  -  http://localhost:${PORT}"
  else
    fail "did not become healthy within 90s"
    return 1
  fi

  warn "upstream keys are optional; unconfigured layers report degraded:"
  [ -n "${EXPLORER_FIRMS_MAP_KEY:-}" ] || dim "  EXPLORER_FIRMS_MAP_KEY unset  ->  wildfires degraded (401)"
  [ -n "${EXPLORER_AIS_API_KEY:-}" ]   || dim "  EXPLORER_AIS_API_KEY unset    ->  vessels degraded"

  if [ "${EIE_OPEN:-1}" = "1" ] && command -v xdg-open >/dev/null 2>&1; then
    xdg-open "http://localhost:${PORT}" >/dev/null 2>&1 || true
  fi
  dim "logs: ./scripts/eie.sh logs -f"
}

cmd_stop() {
  local pid
  if ! pid="$(running_pid)"; then
    info "not running"
    rm -f "${PID_FILE}" 2>/dev/null || true
    return 0
  fi
  info "stopping pid ${pid}"
  # SIGTERM lets Spring run its shutdown hooks and close the AISStream socket cleanly.
  kill "${pid}" 2>/dev/null || true
  # Graceful shutdown waits on the AISStream socket and can take ~20s, so allow headroom
  # before escalating; an unclean kill would drop in-flight requests.
  local i
  for ((i = 0; i < 30; i++)); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      rm -f "${PID_FILE}" 2>/dev/null || true
      ok "stopped"
      return 0
    fi
    sleep 1
  done
  warn "did not exit after 30s; sending SIGKILL"
  kill -9 "${pid}" 2>/dev/null || true
  rm -f "${PID_FILE}" 2>/dev/null || true
  ok "killed"
}

cmd_status() {
  local pid
  if pid="$(running_pid)"; then
    ok "running  pid ${pid}  port ${PORT}"
    if health_ok; then
      ok "healthy"
    else
      warn "not answering /api/v1/system/info yet"
    fi
    dim "uptime: $(ps -o etime= -p "${pid}" 2>/dev/null | tr -d ' ')"
    dim "log:    ${LOG_FILE}"
  else
    info "not running"
    return 1
  fi
}

cmd_logs() {
  [ -f "${LOG_FILE}" ] || die "no log yet at ${LOG_FILE}"
  tail ${1:+-f} "${LOG_FILE}"
}

cmd_clean() {
  if running_pid >/dev/null; then
    die "still running; stop it first"
  fi
  info "removing target/ and .run/"
  rm -rf "${PROJECT_DIR}/target" "${RUN_DIR}"
  ok "clean"
}

# ----------------------------------------------------------------- cesium

# Vendors Cesium into the static root so the dashboard needs no outbound network.
cmd_fetch_cesium() {
  local dest="${PROJECT_DIR}/src/main/resources/static/cesium"
  local tmp
  tmp="$(mktemp -d)"

  info "fetching Cesium ${CESIUM_VERSION} (npm tarball)"
  local tarball="${tmp}/cesium.tgz"
  if ! curl -fsSL --max-time 600 \
        "https://registry.npmjs.org/cesium/-/cesium-${CESIUM_VERSION}.tgz" -o "${tarball}"; then
    die "could not download cesium-${CESIUM_VERSION}.tgz"
  fi

  dim "  extracting"
  tar -xzf "${tarball}" -C "${tmp}"
  local built="${tmp}/package/Build/Cesium"
  [ -d "${built}" ] || die "unexpected tarball layout; expected ${built}"

  rm -rf "${dest}"
  mkdir -p "${dest}"
  # Only the runtime assets: the Sources/ and Build/CesiumUnminified trees are not needed
  # and would add hundreds of megabytes to the working tree.
  # Widgets.css lives at Widgets/widgets.css, which is where the dashboard looks for it.
  for asset in Cesium.js Assets Workers ThirdParty Widgets; do
    [ -e "${built}/${asset}" ] || die "tarball is missing ${asset}"
    cp -R "${built}/${asset}" "${dest}/"
    dim "  ${asset}"
  done

  rm -rf "${tmp}"
  ok "vendored into src/main/resources/static/cesium ($(du -sh "${dest}" | cut -f1))"
  dim "gitignored; rebuild the jar to serve it"
}

usage() {
  sed -n '3,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

main() {
  local command="${1:-help}"
  shift || true
  case "${command}" in
    run)          cmd_run "$@" ;;
    start)        cmd_start "$@" ;;
    stop)         cmd_stop "$@" ;;
    restart)      cmd_stop; cmd_run "$@" ;;
    status)       cmd_status "$@" ;;
    logs)         cmd_logs "$@" ;;
    build)        build "$@" ;;
    fetch-cesium) cmd_fetch_cesium "$@" ;;
    clean)        cmd_clean "$@" ;;
    help|-h|--help) usage ;;
    *)            fail "unknown command: ${command}"; usage; exit 2 ;;
  esac
}

main "$@"
