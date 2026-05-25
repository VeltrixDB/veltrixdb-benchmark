#!/usr/bin/env bash
# run-benchmark.sh — main entry point for VeltrixDB benchmark suite.
#
# Auto-detects the latest VeltrixDB Docker image tag (v* pattern),
# starts Prometheus + Grafana alongside VeltrixDB, then runs the
# selected benchmark profile.
#
# Usage:
#   ./scripts/run-benchmark.sh                          # defaults: mixed profile, 1M keys, 128 B values
#   ./scripts/run-benchmark.sh --profile write-heavy    # write-heavy preset
#   ./scripts/run-benchmark.sh --profile read-heavy     # read-heavy preset
#   ./scripts/run-benchmark.sh --image ghcr.io/veltrixdb/veltrixdb:v1.2.3  # pin version
#   ./scripts/run-benchmark.sh --no-monitoring          # skip Prometheus + Grafana
#   ./scripts/run-benchmark.sh --duration 120           # override run duration (s)
#   ./scripts/run-benchmark.sh --concurrency 64
#   ./scripts/run-benchmark.sh --num-keys 10000000
#   ./scripts/run-benchmark.sh --value-size 256
#   ./scripts/run-benchmark.sh --batch-size 1024
#   ./scripts/run-benchmark.sh --output-dir /tmp/results
#
# Environment overrides (all optional):
#   VELTRIXDB_IMAGE   Full image ref (overrides auto-detect)
#   VELTRIXDB_TAG     Just the tag (e.g. v1.2.3), applied to ghcr.io/veltrixdb/veltrixdb
#   REGISTRY          Image registry prefix (default: ghcr.io/veltrixdb/veltrixdb)
#   GRAFANA_PORT      default 3000
#   PROMETHEUS_PORT   default 9090
#   DB_PORT           default 9000
#   METRICS_PORT      default 2112

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

# ── Defaults ──────────────────────────────────────────────────────────────────
REGISTRY="${REGISTRY:-ghcr.io/veltrixdb/veltrixdb}"
PROFILE="mixed"
MONITORING=true
DURATION=""
CONCURRENCY=""
NUM_KEYS=""
VALUE_SIZE=""
BATCH_SIZE=""
OUTPUT_DIR="${REPO_ROOT}/results/$(date +%Y%m%d-%H%M%S)"
CUSTOM_IMAGE="${VELTRIXDB_IMAGE:-}"

# ── Arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)        PROFILE="$2";      shift 2 ;;
    --image)          CUSTOM_IMAGE="$2"; shift 2 ;;
    --no-monitoring)  MONITORING=false;  shift ;;
    --duration)       DURATION="$2";     shift 2 ;;
    --concurrency)    CONCURRENCY="$2";  shift 2 ;;
    --num-keys)       NUM_KEYS="$2";     shift 2 ;;
    --value-size)     VALUE_SIZE="$2";   shift 2 ;;
    --batch-size)     BATCH_SIZE="$2";   shift 2 ;;
    --output-dir)     OUTPUT_DIR="$2";   shift 2 ;;
    --help|-h)
      sed -n '3,25p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *)
      echo "Unknown option: $1  (use --help for usage)" >&2
      exit 1 ;;
  esac
done

GRAFANA_PORT="${GRAFANA_PORT:-3000}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
DB_PORT="${DB_PORT:-9000}"
METRICS_PORT="${METRICS_PORT:-2112}"

# ── Helpers ───────────────────────────────────────────────────────────────────
say()  { printf '\033[1m%s\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
ok()   { printf '  \033[32m✓\033[0m %s\n' "$*"; }
err()  { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; }
die()  { err "$*"; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1 — please install it and re-run."
}

# ── Prerequisites ─────────────────────────────────────────────────────────────
require_cmd docker
require_cmd docker-compose || require_cmd "docker compose"
require_cmd python3
require_cmd curl

DOCKER_COMPOSE="docker-compose"
if ! command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker compose"
fi

# ── Auto-detect latest VeltrixDB image ───────────────────────────────────────
# NOTE: all status messages inside this function MUST go to stderr (>&2)
# because the function is called via $() and its stdout becomes VELTRIXDB_IMAGE.
resolve_image() {
  if [[ -n "${CUSTOM_IMAGE}" ]]; then
    echo "${CUSTOM_IMAGE}"
    return
  fi

  if [[ -n "${VELTRIXDB_TAG:-}" ]]; then
    echo "${REGISTRY}:${VELTRIXDB_TAG}"
    return
  fi

  say "Auto-detecting latest VeltrixDB image tag..." >&2

  local latest_tag=""
  local api_url="https://ghcr.io/v2/veltrixdb/veltrixdb/tags/list"

  # GHCR v2 API requires a Bearer token even for public images.
  # Exchange anonymous credentials for a pull-scoped token first.
  local ghcr_token response
  ghcr_token=$(curl -fsSL --max-time 10 \
    "https://ghcr.io/token?scope=repository:veltrixdb/veltrixdb:pull&service=ghcr.io" \
    2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))" 2>/dev/null \
    || echo "")

  if [[ -n "${ghcr_token}" ]]; then
    response=$(curl -fsSL --max-time 10 \
      -H "Authorization: Bearer ${ghcr_token}" \
      -H "Accept: application/json" \
      "${api_url}" 2>/dev/null || echo "")

    if [[ -n "${response}" ]]; then
      latest_tag=$(echo "${response}" \
        | python3 -c "
import sys, json, re
data = json.load(sys.stdin)
tags = [t for t in data.get('tags', []) if re.match(r'^v\d+\.\d+\.\d+', t)]
def semver_key(tag):
    m = re.match(r'^v?(\d+)\.(\d+)\.(\d+)', tag)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else (0,0,0)
tags.sort(key=semver_key, reverse=True)
print(tags[0] if tags else '')
" 2>/dev/null || echo "")
    fi
  fi

  if [[ -z "${latest_tag}" ]]; then
    # Fallback: GitHub releases API (works for public repos without auth)
    response=$(curl -fsSL --max-time 10 \
      -H "Accept: application/vnd.github+json" \
      "https://api.github.com/repos/VeltrixDB/VeltrixDB/releases/latest" \
      2>/dev/null || echo "")

    if [[ -n "${response}" ]]; then
      latest_tag=$(echo "${response}" \
        | python3 -c "
import sys, json, re
d = json.load(sys.stdin)
tag = d.get('tag_name', '')
print(tag if re.match(r'^v\d+', tag) else '')
" 2>/dev/null || echo "")
    fi
  fi

  if [[ -z "${latest_tag}" ]]; then
    info "Could not auto-detect tag; falling back to 'latest'" >&2
    echo "${REGISTRY}:latest"
  else
    ok "Latest tag: ${latest_tag}" >&2
    echo "${REGISTRY}:${latest_tag}"
  fi
}

# ── Load benchmark profile ─────────────────────────────────────────────────────
PROFILE_FILE="${REPO_ROOT}/config/benchmark-profiles/${PROFILE}.yaml"
[[ -f "${PROFILE_FILE}" ]] || die "Profile not found: ${PROFILE_FILE}"

say "Loading profile: ${PROFILE}"
info "File: ${PROFILE_FILE}"

# Parse YAML profile using Python (no yq dependency required)
parse_profile() {
  python3 - "${PROFILE_FILE}" <<'PYEOF'
import sys, re

path = sys.argv[1]
with open(path) as f:
    lines = f.readlines()

section = None
for line in lines:
    stripped = line.rstrip()
    if re.match(r'^[a-zA-Z]', stripped):
        section = stripped.rstrip(':')
    m = re.match(r'^\s{2,}(\w+):\s*(.*)', stripped)
    if m and section == 'benchmark':
        key = m.group(1).upper().replace('-', '_')
        val = m.group(2).strip().strip('"').strip("'")
        if val and not val.startswith('#'):
            print(f"PROFILE_{key}={val}")
    if m and section == 'veltrixdb':
        key = m.group(1).upper().replace('-', '_')
        val = m.group(2).strip().strip('"').strip("'")
        if val and not val.startswith('#'):
            print(f"VDB_{key}={val}")
PYEOF
}

eval "$(parse_profile)"

# CLI overrides take priority over profile values
DURATION="${DURATION:-${PROFILE_DURATION:-60}}"
CONCURRENCY="${CONCURRENCY:-${PROFILE_CONCURRENCY:-16}}"
NUM_KEYS="${NUM_KEYS:-${PROFILE_NUM_KEYS:-1000000}}"
VALUE_SIZE="${VALUE_SIZE:-${PROFILE_VALUE_SIZE:-128}}"
BATCH_SIZE="${BATCH_SIZE:-${PROFILE_BATCH_SIZE:-256}}"
MODE="${PROFILE_MODE:-mixed}"
READ_RATIO="${PROFILE_READ_RATIO:-0.7}"
WARMUP_DURATION="${PROFILE_WARMUP_DURATION:-10}"

VDB_CACHE_MB="${VDB_CACHE_MB:-256}"
VDB_GC_THRESHOLD="${VDB_GC_THRESHOLD:-0.30}"
VDB_WAL_FLUSH_WINDOW_MS="${VDB_WAL_FLUSH_WINDOW_MS:-10}"

info "Mode:        ${MODE}"
info "Duration:    ${DURATION}s"
info "Concurrency: ${CONCURRENCY}"
info "Keys:        ${NUM_KEYS}"
info "Value size:  ${VALUE_SIZE}B"
info "Batch size:  ${BATCH_SIZE}"
info "Read ratio:  ${READ_RATIO}"

# ── Resolve Docker image ───────────────────────────────────────────────────────
VELTRIXDB_IMAGE="$(resolve_image)"
say "Using image: ${VELTRIXDB_IMAGE}"

# Write resolved image to .env for docker-compose
cat > "${REPO_ROOT}/.env" <<ENVEOF
VELTRIXDB_IMAGE=${VELTRIXDB_IMAGE}
DB_PORT=${DB_PORT}
METRICS_PORT=${METRICS_PORT}
GRAFANA_PORT=${GRAFANA_PORT}
PROMETHEUS_PORT=${PROMETHEUS_PORT}
VDB_CACHE_MB=${VDB_CACHE_MB}
VDB_GC_THRESHOLD=${VDB_GC_THRESHOLD}
VDB_WAL_FLUSH_WINDOW_MS=${VDB_WAL_FLUSH_WINDOW_MS}
ENVEOF

# ── Start stack ───────────────────────────────────────────────────────────────
say "Starting VeltrixDB stack..."

COMPOSE_PROFILES="veltrixdb"
if [[ "${MONITORING}" == "true" ]]; then
  COMPOSE_PROFILES="veltrixdb,monitoring"
fi

$DOCKER_COMPOSE --profile veltrixdb up -d --pull missing

if [[ "${MONITORING}" == "true" ]]; then
  $DOCKER_COMPOSE --profile monitoring up -d --pull missing
fi

# ── Wait for VeltrixDB to be ready ────────────────────────────────────────────
say "Waiting for VeltrixDB to be ready..."
for i in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:${METRICS_PORT}/readyz" >/dev/null 2>&1; then
    ok "VeltrixDB ready"
    break
  fi
  if [[ $i -eq 60 ]]; then
    err "VeltrixDB did not become ready after 30s"
    $DOCKER_COMPOSE logs veltrixdb | tail -30
    exit 1
  fi
  printf '.'
  sleep 0.5
done

if [[ "${MONITORING}" == "true" ]]; then
  say "Grafana dashboard: http://localhost:${GRAFANA_PORT}  (admin/admin)"
  say "Prometheus:        http://localhost:${PROMETHEUS_PORT}"
fi

# ── Pull latest Python client ─────────────────────────────────────────────────
mkdir -p "${OUTPUT_DIR}"
say "Output directory: ${OUTPUT_DIR}"

# ── Run benchmark ─────────────────────────────────────────────────────────────
say "Running benchmark: ${MODE} mode"

BENCH_ARGS=(
  --host "127.0.0.1"
  --port "${DB_PORT}"
  --metrics-url "http://127.0.0.1:${METRICS_PORT}/metrics"
  --mode "${MODE}"
  --duration "${DURATION}"
  --concurrency "${CONCURRENCY}"
  --num-keys "${NUM_KEYS}"
  --value-size "${VALUE_SIZE}"
  --batch-size "${BATCH_SIZE}"
  --read-ratio "${READ_RATIO}"
  --warmup "${WARMUP_DURATION}"
  --output-dir "${OUTPUT_DIR}"
)

python3 "${REPO_ROOT}/benchmarks/workload_generator.py" "${BENCH_ARGS[@]}" 2>&1 | tee "${OUTPUT_DIR}/benchmark.log"

# ── Print results ─────────────────────────────────────────────────────────────
say "Benchmark complete. Results in: ${OUTPUT_DIR}"
if [[ -f "${OUTPUT_DIR}/summary.json" ]]; then
  echo ""
  python3 - "${OUTPUT_DIR}/summary.json" <<'PYEOF'
import json, sys
with open(sys.argv[1]) as f:
    s = json.load(f)
w = s.get('write', {})
r = s.get('read', {})
print(f"{'Metric':<35} {'Write':>14} {'Read':>14}")
print("─" * 65)
for k in ('ops_per_sec', 'p50_ms', 'p95_ms', 'p99_ms', 'errors'):
    label = k.replace('_', ' ').title()
    wv = w.get(k, '—')
    rv = r.get(k, '—')
    wf = f"{wv:>14.1f}" if isinstance(wv, float) else f"{wv:>14}"
    rf = f"{rv:>14.1f}" if isinstance(rv, float) else f"{rv:>14}"
    print(f"  {label:<33}{wf}{rf}")
print()
PYEOF
fi

if [[ "${MONITORING}" == "true" ]]; then
  say "Grafana dashboard: http://localhost:${GRAFANA_PORT}  (admin/admin)"
fi
