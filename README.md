# VeltrixDB Benchmark

Standalone benchmark suite for evaluating [VeltrixDB](https://github.com/VeltrixDB/veltrixdb) performance with one-command setup, built-in Prometheus metrics, and pre-configured Grafana dashboards.

## Quick Start

```bash
# Clone the repo
git clone https://github.com/VeltrixDB/veltrixdb-benchmark
cd veltrixdb-benchmark

# Run with all defaults (mixed 70R/30W, 60s, 16 workers, 1M keys, 128B values)
./scripts/run-benchmark.sh

# Open Grafana dashboard → http://localhost:3000  (admin / admin)
# Open Prometheus      → http://localhost:9090
```

The script auto-detects the latest VeltrixDB Docker image (pattern `v*`) from the GitHub Container Registry, starts Prometheus + Grafana alongside VeltrixDB, runs the benchmark, and prints a results table.

---

## Prerequisites

| Tool | Notes |
|------|-------|
| Docker ≥ 24 | `docker --version` |
| Docker Compose v2 | `docker compose version` |
| Python ≥ 3.9 | `python3 --version` |
| curl | for health checks and metrics queries |

No Python packages are required — the benchmark uses only the standard library.

---

## Benchmark Profiles

```bash
./scripts/run-benchmark.sh --profile write-heavy    # bulk-load, MPUT-1024, tight WAL window
./scripts/run-benchmark.sh --profile read-heavy     # warm-cache, MGET-64, high concurrency
./scripts/run-benchmark.sh --profile mixed          # 70R/30W, default
./scripts/run-benchmark.sh --profile balanced       # 50R/50W, MPUT/MGET-256
```

Profile files are in `config/benchmark-profiles/`.

---

## CLI Options

```
./scripts/run-benchmark.sh [options]

  --profile <name>       Benchmark profile (write-heavy|read-heavy|mixed|balanced)
  --image <ref>          Full Docker image reference (skips auto-detect)
  --no-monitoring        Skip Prometheus + Grafana (VeltrixDB only)
  --duration <s>         Override benchmark duration in seconds
  --concurrency <n>      Override number of concurrent workers
  --num-keys <n>         Override keyspace size
  --value-size <bytes>   Override value size
  --batch-size <n>       Override MPUT/MGET batch size (1 = single ops)
  --output-dir <path>    Override results directory (default: results/<timestamp>)
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `VELTRIXDB_IMAGE` | *(auto-detect)* | Full image ref, e.g. `ghcr.io/veltrixdb/veltrixdb:v1.2.3` |
| `VELTRIXDB_TAG` | *(auto-detect)* | Just the tag, e.g. `v1.2.3` |
| `DB_PORT` | `9000` | VeltrixDB TCP port |
| `METRICS_PORT` | `2112` | VeltrixDB metrics + admin port |
| `GRAFANA_PORT` | `3000` | Grafana port |
| `PROMETHEUS_PORT` | `9090` | Prometheus port |

---

## Running Individual Benchmarks

Each benchmark can also be run directly against any running VeltrixDB instance.

### Write Benchmark

```bash
# Single PUT, 16 workers
python3 benchmarks/write_benchmark.py \
  --host 127.0.0.1 --port 9000 \
  --concurrency 16 --duration 60 \
  --value-size 128

# Batched MPUT-1024 (engages VLog block packing, ~25× density gain)
python3 benchmarks/write_benchmark.py \
  --batch-size 1024 --concurrency 8 --duration 60
```

### Read Benchmark

```bash
# Random GET with 32 workers, populate data first
python3 benchmarks/read_benchmark.py \
  --populate --num-keys 500000 --value-size 128 \
  --concurrency 32 --duration 60

# MGET-64 batches, hotspot access pattern
python3 benchmarks/read_benchmark.py \
  --batch-size 64 --access-pattern hotspot --duration 60
```

### Compaction / GC Benchmark

```bash
# Fill 500K keys, overwrite 3× to create dead space, watch GC reclaim
python3 benchmarks/compaction_benchmark.py \
  --fill-keys 500000 --overwrite-rounds 3 \
  --value-size 512 --settle-time 120
```

### Workload Generator (YCSB-compatible)

```bash
# YCSB Workload A: 50% R / 50% W
python3 benchmarks/workload_generator.py --ycsb A --duration 60

# YCSB Workload B: 95% R / 5% W
python3 benchmarks/workload_generator.py --ycsb B --duration 60

# YCSB Workload C: 100% read-only
python3 benchmarks/workload_generator.py --ycsb C --duration 60

# Custom: 80% reads, batched
python3 benchmarks/workload_generator.py \
  --mode mixed --read-ratio 0.8 --batch-size 128 \
  --concurrency 16 --duration 90
```

---

## Results

All benchmark scripts write results to `results/` (or `--output-dir`):

| File | Contents |
|------|----------|
| `summary.json` | Latest run summary (overwritten each run) |
| `workload_<ts>.json` | Detailed per-run results with server metric deltas |
| `workload_<ts>.csv` | CSV-friendly row for spreadsheet comparison |
| `compaction_<ts>.json` | GC benchmark summary |
| `compaction_timeseries_<ts>.csv` | GC metric samples over time |
| `benchmark.log` | Full console output |

### Example summary.json

```json
{
  "mode": "mixed",
  "total_ops": 2847293,
  "total_ops_per_sec": 47454.9,
  "read": {
    "ops_per_sec": 33218.4,
    "p50_ms": 0.182,
    "p95_ms": 0.611,
    "p99_ms": 1.243
  },
  "write": {
    "ops_per_sec": 14236.5,
    "p50_ms": 8.412,
    "p95_ms": 12.871,
    "p99_ms": 18.334
  }
}
```

---

## Monitoring Stack

### Grafana Dashboard

The pre-built dashboard at `grafana/dashboards/veltrixdb-overview.json` includes:

| Section | Panels |
|---------|--------|
| **Overview** | Write ops/s, Read ops/s, Cache hit rate, Garbage ratio, Emergency GC count, Active connections |
| **Write Performance** | Throughput timeseries, Latency P50/P95/P99 |
| **Read Performance** | Throughput timeseries, Latency P50/P95/P99 |
| **Cache Performance** | Hit rate curve, hits vs misses, cache utilization gauge |
| **GC & Compaction** | GC activity, garbage ratio curve, bytes reclaimed/s |
| **Storage** | VLog file size, index size (keys), Bloom filter FP rate |
| **WAL & Admission** | WAL group-commit batch size, write throttle rate, network I/O |
| **GC Diagnostics** | GC skip reasons (ratio / paused / empty), GC read errors, CAS fail rate |

Dashboard auto-refreshes every 5 seconds.

### Prometheus Recording Rules

`prometheus/recording-rules.yml` pre-computes:

- `veltrixdb:write_ops_per_second` / `veltrixdb:read_ops_per_second`
- `veltrixdb:write_p50/p95/p99_seconds` / `veltrixdb:read_p50/p95/p99_seconds`
- `veltrixdb:cache_hit_rate` / `veltrixdb:cache_utilization`
- `veltrixdb:gc_runs_per_minute` / `veltrixdb:gc_bytes_reclaimed_per_second`
- `veltrixdb:write_throttle_rate` / `veltrixdb:throttle_fraction`
- `veltrixdb:wal_flushes_per_second` / `veltrixdb:wal_avg_batch_size`

---

## Tuning Reference

VeltrixDB configuration is passed via CLI flags in `docker-compose.yml`. Key levers:

| Flag | Default | Effect |
|------|---------|--------|
| `-cache` | `256` MB | LIRS cache size. Increase for read-heavy workloads. |
| `-wal-flush-window-ms` | `10` | WAL group-commit window. Lower = lower latency. Higher = higher throughput. Must equal `-vlog-flush-window-ms`. |
| `-vlog-flush-window-ms` | `10` | VLog group-commit window. Must equal WAL window. |
| `-gc-threshold` | `0.30` | VLog dead-space ratio to trigger GC. |

Override via environment variables in `.env` or via `--` flags to `run-benchmark.sh`:

```bash
VDB_CACHE_MB=4096 VDB_WAL_FLUSH_WINDOW_MS=5 ./scripts/run-benchmark.sh --profile write-heavy
```

### Performance Reference Points

| Workload | Hardware | Throughput | P99 Latency |
|----------|----------|------------|-------------|
| MPUT-1024, 128B values | macOS M-series (dev) | ~360K entries/s | ~9 ms |
| MPUT-1024, 128B values | Linux n2-highmem-64 (8 NVMe) | ~3M entries/s (projected) | ~5.2 ms |
| GET cache-hit | macOS M-series | ~1.44M reads/s | <1 ms |
| GET cache-hit | Linux n2-highmem-64 | ~2M reads/s target | <5 ms |

> **macOS note**: `F_FULLFSYNC` costs ~7–10 ms vs ~0.2–0.5 ms on Linux NVMe. Write latency on macOS is hardware-limited, not code-limited.

---

## Repository Structure

```
veltrixdb-benchmark/
├── docker-compose.yml           # VeltrixDB + Prometheus + Grafana
├── README.md
├── benchmarks/
│   ├── client.py                # Embedded VeltrixDB binary protocol client
│   ├── write_benchmark.py       # Write throughput + latency (PUT / MPUT)
│   ├── read_benchmark.py        # Read throughput + cache hit rate (GET / MGET)
│   ├── compaction_benchmark.py  # GC efficiency: fill → overwrite → settle
│   └── workload_generator.py    # YCSB-compatible mixed workloads
├── config/
│   ├── veltrixdb-config.yaml    # Default server settings (reference only)
│   └── benchmark-profiles/
│       ├── write-heavy.yaml
│       ├── read-heavy.yaml
│       ├── mixed.yaml           # default
│       └── balanced.yaml
├── grafana/
│   ├── dashboards/
│   │   └── veltrixdb-overview.json
│   └── provisioning/
│       ├── datasources/prometheus.yaml
│       └── dashboards/dashboard.yaml
├── prometheus/
│   ├── prometheus.yml
│   └── recording-rules.yml
└── scripts/
    ├── run-benchmark.sh         # Main entry point
    └── cleanup.sh               # Stop + optional purge
```

---

## Cleanup

```bash
./scripts/cleanup.sh           # stop containers, keep volumes and results
./scripts/cleanup.sh --purge   # stop + remove volumes (prompts before deleting results)
```

---

## License

MIT
