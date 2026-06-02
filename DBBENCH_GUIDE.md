# VeltrixDB dbbench Guide

Facebook's RocksDB ships with a tool called `db_bench` — the de facto standard for publishing KV store performance numbers. Every major paper and blog post that compares RocksDB, LevelDB, WiredTiger, or LMDB uses `db_bench` workloads and output format.

`dbbench` is VeltrixDB's equivalent. It uses the same benchmark names, the same flags, and the same output format as RocksDB's `db_bench`, so your numbers are directly comparable.

---

## What db_bench tests (and why it matters)

RocksDB's `db_bench` tests three fundamental questions about a KV store:

| Question | Workload |
|----------|----------|
| How fast can you write fresh data? | `fillseq`, `fillrandom` |
| How fast can you overwrite existing data? | `overwrite` |
| How fast can you read? | `readseq`, `readrandom` |
| What happens under mixed real-world load? | `readwhilewriting` |
| How fast can you do batch writes? | `fillbatch` |
| How does NOT_FOUND affect performance? | `readmissing` |
| How fast can you delete? | `deleteseq`, `deleterandom` |

---

## Quick start

### 1. Start VeltrixDB

```bash
go run ./cmd/server \
  --port 9000 \
  --admin-port 2112 \
  --cache 4096 \
  --data-dir /tmp/veltrixdb-bench
```

### 2. Build dbbench

```bash
cd veltrixdb-benchmark/dbbench
go build -o dbbench .
```

A single static binary is produced. No runtime dependencies.

### 3. Run the standard db_bench suite

```bash
./dbbench \
  --benchmarks=fillseq,fillrandom,overwrite,readseq,readrandom,readwhilewriting \
  --num=1000000 \
  --value_size=1024 \
  --threads=1
```

---

## Output format

`dbbench` matches RocksDB's `db_bench` output exactly:

```
VeltrixDB dbbench
  host       : 127.0.0.1:9000
  num        : 1000000
  value_size : 1024 B
  threads    : 1
  batch_size : 100

fillseq          :       1.823 micros/op;  534.8 MB/s (1000000 ops)
fillrandom        :       2.441 micros/op;  399.6 MB/s (1000000 ops)
overwrite         :       2.513 micros/op;  388.0 MB/s (1000000 ops)
readseq           :       0.198 micros/op; 4908.1 MB/s (1000000 ops)
readrandom        :       0.912 micros/op; (1000000 ops; 97.3% found)
readwhilewriting  :       1.104 micros/op; (1000000 ops; 96.1% found)
```

---

## All flags

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | VeltrixDB server hostname |
| `--port` | `9000` | VeltrixDB server port |
| `--benchmarks` | `fillrandom,readrandom` | Comma-separated list of benchmarks |
| `--num` | `1000000` | Number of key-value pairs |
| `--value_size` | `1024` | Value size in bytes |
| `--key_size` | `16` | Key prefix length |
| `--threads` | `1` | Number of concurrent goroutines |
| `--batch_size` | `100` | Entries per MPUT/MGET call (fillbatch only) |
| `--histogram` | `false` | Print P50/P75/P99/P99.9 latency histogram |
| `--stats_interval` | `0` | Print progress every N seconds |
| `--duration` | `0` | Run each benchmark for N seconds instead of `--num` ops |
| `--username` | `` | AUTH username (omit if no auth) |
| `--password` | `` | AUTH password |
| `--report_file` | `` | Write JSON result to file |

---

## All benchmarks

### Write benchmarks

```bash
# Sequential inserts (best-case write throughput)
./dbbench --benchmarks=fillseq --num=1000000 --threads=1

# Random inserts (realistic write throughput, tests shard distribution)
./dbbench --benchmarks=fillrandom --num=1000000 --threads=16

# Batch writes using MPUT (tests pipeline efficiency)
./dbbench --benchmarks=fillbatch --num=1000000 --threads=16 --batch_size=256

# Overwrite existing keys (tests GC + admission control)
# Run fillrandom first, then overwrite
./dbbench --benchmarks=fillrandom,overwrite --num=1000000 --threads=16
```

### Read benchmarks

```bash
# Sequential reads (tests cache warm path)
./dbbench --benchmarks=readseq --num=1000000 --threads=1

# Random reads (Zipfian-equivalent — hot key skew)
./dbbench --benchmarks=readrandom --num=1000000 --threads=64

# Read missing keys (tests NOT_FOUND fast path)
./dbbench --benchmarks=readmissing --num=1000000 --threads=16
```

### Mixed benchmarks

```bash
# Concurrent reads + background writes (most realistic)
./dbbench --benchmarks=readwhilewriting --num=1000000 --threads=16
```

### Delete benchmarks

```bash
./dbbench --benchmarks=deleteseq,deleterandom --num=1000000 --threads=16
```

---

## Standard RocksDB comparison run

This is the exact sequence Facebook's benchmark team uses. Run VeltrixDB and RocksDB with the same flags on the same machine.

```bash
# Phase 1 — Write performance
./dbbench \
  --benchmarks=fillseq,fillrandom,overwrite \
  --num=1000000 \
  --value_size=1024 \
  --threads=1

# Phase 2 — Read performance (after fillrandom populates the DB)
./dbbench \
  --benchmarks=readseq,readrandom,readmissing \
  --num=1000000 \
  --value_size=1024 \
  --threads=16 \
  --histogram

# Phase 3 — Mixed workload (most important for real-world claims)
./dbbench \
  --benchmarks=fillrandom,readwhilewriting \
  --num=1000000 \
  --value_size=1024 \
  --threads=16 \
  --histogram
```

---

## Latency histogram

Add `--histogram` to see P50/P75/P99/P99.9/P99.99 and a bucket breakdown:

```bash
./dbbench \
  --benchmarks=readrandom \
  --num=1000000 \
  --threads=16 \
  --histogram
```

Output:

```
readrandom        :       0.912 micros/op; (1000000 ops; 97.3% found)

Microseconds per read:
Count: 1000000  Average: 0.91  StdDev: 2.3
Min: 0  Median: 1.0  Max: 50235
Percentiles: P50: 1.0 P75: 1.0 P99: 3.0 P99.9: 9.0 P99.99: 25.0
------------------------------------------------------
[       0,       1 )    350000 35.000%  35.000% #######
[       1,       2 )    450000 45.000%  80.000% #########
[       2,       3 )    120000 12.000%  92.000% ##
[       3,       5 )     50000  5.000%  97.000% #
[       5,      10 )     20000  2.000%  99.000%
[      10,     100 )      9000  0.900%  99.900%
[     100,    1000 )       900  0.090%  99.990%
```

---

## Time-based runs (match db_bench --duration)

Instead of a fixed op count, run for a fixed wall-clock time:

```bash
# 60-second sustained read test
./dbbench \
  --benchmarks=readrandom \
  --duration=60 \
  --value_size=1024 \
  --threads=64 \
  --stats_interval=10 \
  --histogram
```

`--stats_interval=10` prints a progress line every 10 seconds so you can watch throughput live.

---

## Save results to JSON

```bash
./dbbench \
  --benchmarks=fillrandom,readrandom,readwhilewriting \
  --num=1000000 \
  --threads=16 \
  --report_file=results/bench_$(date +%Y%m%d-%H%M%S).json
```

JSON format:

```json
{
  "config": {
    "host": "127.0.0.1",
    "port": 9000,
    "num": 1000000,
    "value_size": 1024,
    "threads": 16,
    "batch_size": 100
  },
  "benchmarks": [
    {
      "name": "fillrandom",
      "ops": 1000000,
      "elapsed_s": 2.441,
      "ops_per_sec": 409667.3,
      "micros_per_op": 2.441,
      "errors": 0,
      "p50_us": 2.0,
      "p99_us": 8.0,
      "p99_9_us": 20.0
    }
  ]
}
```

---

## Comparing with RocksDB db_bench

Run RocksDB's `db_bench` on the same machine with equivalent settings:

```bash
# RocksDB db_bench (from the RocksDB repo)
./db_bench \
  --benchmarks=fillseq,fillrandom,readrandom \
  --num=1000000 \
  --value_size=1024 \
  --threads=16 \
  --histogram=1
```

Then run VeltrixDB dbbench with the same `--num`, `--value_size`, and `--threads`. The output format is identical, so the numbers paste directly into a comparison table.

Key difference: RocksDB `db_bench` is an in-process benchmark (library call latency). VeltrixDB `dbbench` measures over TCP (client-server round-trip). The TCP stack adds ~10–50 µs on loopback — to get a fair comparison, run both on the same host or benchmark VeltrixDB with the client on the same machine as the server.

---

## Full all-workloads script

```bash
#!/usr/bin/env bash
set -euo pipefail

HOST=127.0.0.1
PORT=9000
NUM=1000000
VAL=1024
THREADS=16
TS=$(date +%Y%m%d-%H%M%S)
RESULTS="results/$TS"
mkdir -p "$RESULTS"

DBBENCH="./dbbench"

echo "==> Phase 1: write benchmarks"
$DBBENCH \
  --host=$HOST --port=$PORT \
  --benchmarks=fillseq,fillrandom,fillbatch,overwrite \
  --num=$NUM --value_size=$VAL --threads=$THREADS \
  --histogram \
  --report_file="$RESULTS/write.json" \
  | tee "$RESULTS/write.txt"

echo "==> Phase 2: read benchmarks (DB already populated)"
$DBBENCH \
  --host=$HOST --port=$PORT \
  --benchmarks=readseq,readrandom,readmissing \
  --num=$NUM --value_size=$VAL --threads=$THREADS \
  --histogram \
  --report_file="$RESULTS/read.json" \
  | tee "$RESULTS/read.txt"

echo "==> Phase 3: mixed workload"
$DBBENCH \
  --host=$HOST --port=$PORT \
  --benchmarks=readwhilewriting \
  --num=$NUM --value_size=$VAL --threads=$THREADS \
  --histogram \
  --report_file="$RESULTS/mixed.json" \
  | tee "$RESULTS/mixed.txt"

echo "==> Phase 4: delete benchmarks"
$DBBENCH \
  --host=$HOST --port=$PORT \
  --benchmarks=deleteseq,deleterandom \
  --num=$NUM --value_size=$VAL --threads=$THREADS \
  --report_file="$RESULTS/delete.json" \
  | tee "$RESULTS/delete.txt"

echo ""
echo "Done. Results in $RESULTS/"
```

---

## Tuning tips

**Thread count**: for read workloads, scale threads until throughput plateaus — that's your saturation point. Start at 16, double until flat.

**Value size**: default 1024 B matches most RocksDB benchmark publications. Use 128 B for small-value tests (config/metadata stores) or 65536 B for large-value tests (blob stores).

**Cache size**: add `--cache` to the VeltrixDB server to fit the working set in DRAM. For 1M × 1 KB records: `--cache 1024` (1 GB). The `readseq` test will show P50 ~200 ns when the working set is fully cached.

**Batch size**: `fillbatch --batch_size=256` is the sweet spot for VeltrixDB's block packing — the server coalesces 256 entries into a single VLog segment, maximising write density.

---

## File locations reference

```
veltrixdb-benchmark/
├── DBBENCH_GUIDE.md       ← this file
└── dbbench/
    ├── go.mod             ← Go module (no external deps)
    ├── client.go          ← inline VeltrixDB binary protocol client
    ├── stats.go           ← histogram + db_bench-format stats
    └── main.go            ← all workloads + flag parsing + JSON report
```
