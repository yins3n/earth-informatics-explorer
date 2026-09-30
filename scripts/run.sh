#!/usr/bin/env bash
#
# Convenience wrapper around `./scripts/eie.sh run`: build if needed, then start the server
# detached in the background and print where the log and pid file are. The command returns as
# soon as the server is healthy, it does not stay in the foreground.
#
# To follow the log:   ./scripts/eie.sh logs -f
# To stop the server:  ./scripts/eie.sh stop
# For a blocking run:  ./scripts/eie.sh run  (with FOREGROUND=1, see eie.sh)
#
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/eie.sh" run "$@"
