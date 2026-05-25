#!/usr/bin/env bash
# cleanup.sh — stop all benchmark containers and optionally remove volumes.
#
# Usage:
#   ./scripts/cleanup.sh          # stop containers, keep volumes
#   ./scripts/cleanup.sh --purge  # stop + remove volumes + results

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

PURGE=false
[[ "${1:-}" == "--purge" ]] && PURGE=true

DOCKER_COMPOSE="docker-compose"
command -v docker-compose >/dev/null 2>&1 || DOCKER_COMPOSE="docker compose"

say() { printf '\033[1m%s\033[0m\n' "$*"; }
ok()  { printf '  \033[32m✓\033[0m %s\n' "$*"; }

say "Stopping benchmark stack..."
$DOCKER_COMPOSE --profile veltrixdb --profile monitoring down 2>/dev/null || true
ok "Containers stopped"

if [[ "${PURGE}" == "true" ]]; then
  say "Removing volumes..."
  $DOCKER_COMPOSE --profile veltrixdb --profile monitoring down -v 2>/dev/null || true
  ok "Volumes removed"

  if [[ -d "${REPO_ROOT}/results" ]]; then
    read -rp "  Delete results directory? [y/N] " confirm
    if [[ "${confirm,,}" == "y" ]]; then
      rm -rf "${REPO_ROOT}/results"
      ok "Results removed"
    fi
  fi
fi

[[ -f "${REPO_ROOT}/.env" ]] && rm -f "${REPO_ROOT}/.env"
say "Done."
