# VeltrixDB YCSB Benchmarking Guide

YCSB (Yahoo Cloud Serving Benchmark) is the industry standard for comparing KV stores. Redis, ScyllaDB, Cassandra, DynamoDB — all publish YCSB numbers. This guide walks you through running official YCSB workloads against VeltrixDB so your results are directly comparable.

---

## What you will run

| Workload | Mix | Use case |
|----------|-----|----------|
| A | 50% R / 50% W | Session store (write-heavy) |
| B | 95% R / 5% W | Photo tagging (read-mostly) |
| C | 100% R | User profile cache (read-only) |
| D | 95% R / 5% insert | Activity stream (read-latest) |
| E | 95% scan / 5% insert | Thread reads (short ranges) |
| F | 50% R / 50% RMW | User DB (read-modify-write) |

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Java | 11+ | `brew install openjdk@17` |
| Maven | 3.8+ | `brew install maven` |
| Python | 3.8+ | Already on macOS |
| VeltrixDB server | any | Built from this repo |

---

## Step 1 — Start VeltrixDB

```bash
# From the VeltrixDB repo root
go run ./cmd/server \
  --port 9000 \
  --admin-port 2112 \
  --cache 4096 \
  --data-dir /tmp/veltrixdb-bench
```

Verify it is up:

```bash
echo "PING" | nc 127.0.0.1 9000
# expected: PONG
```

---

## Step 2 — Install the VeltrixDB Java client to your local Maven repo

The YCSB binding depends on the VeltrixDB Java client JAR. Install it once:

```bash
cd /path/to/VeltrixDB/Veltrixdb-client/java
mvn install -DskipTests
```

You should see `BUILD SUCCESS` and the artifact is stored at:
`~/.m2/repository/com/veltrixdb/veltrixdb-client/1.0.0/`

---

## Step 3 — Build the YCSB binding fat JAR

```bash
cd veltrixdb-benchmark/ycsb-binding
mvn package -DskipTests
```

This produces two JARs in `target/`:
- `veltrixdb-ycsb-binding-1.0.0.jar` — thin jar (needs client on classpath)
- `veltrixdb-ycsb-binding-1.0.0-all.jar` — **fat jar** (use this one)

---

## Step 4 — Download YCSB

```bash
curl -Lo ycsb-0.17.0.tar.gz \
  https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar xf ycsb-0.17.0.tar.gz
cd ycsb-0.17.0
```

---

## Step 5 — Install the binding into YCSB

YCSB looks for binding JARs in `<ycsb-root>/<binding-name>/lib/`. Create the directory and drop the fat JAR in:

```bash
mkdir -p veltrixdb/lib
cp /path/to/veltrixdb-benchmark/ycsb-binding/target/veltrixdb-ycsb-binding-1.0.0-all.jar \
   veltrixdb/lib/
```

---

## Step 6 — Copy workload files

```bash
cp /path/to/veltrixdb-benchmark/ycsb-binding/workloads/workload_* workloads/
```

---

## Step 7 — Load data (one-time)

Before running any workload you must populate the database. The load phase inserts `recordcount` records using 100% inserts.

```bash
# Load Workload A dataset (1M records, 1 KB each = ~1 GB)
bin/ycsb load veltrixdb \
  -s \
  -P workloads/workload_a \
  -p veltrixdb.host=127.0.0.1 \
  -p veltrixdb.port=9000 \
  -threads 32

# Expected output:
# [INSERT], Operations, 1000000
# [INSERT], AverageLatency(us), ...
# [INSERT], Throughput(ops/sec), ...
```

> The same dataset is shared by workloads A, B, C, D, F. Load only once. Workload E uses a separate dataset (different key space) so load it separately if you want to run E.

---

## Step 8 — Run workloads

### Workload A — Update heavy (50R/50W)

```bash
bin/ycsb run veltrixdb \
  -s \
  -P workloads/workload_a \
  -p veltrixdb.host=127.0.0.1 \
  -p veltrixdb.port=9000 \
  -threads 64
```

### Workload B — Read mostly (95R/5W)

```bash
bin/ycsb run veltrixdb \
  -s \
  -P workloads/workload_b \
  -p veltrixdb.host=127.0.0.1 \
  -p veltrixdb.port=9000 \
  -threads 64
```

### Workload C — Read only (100R)

```bash
bin/ycsb run veltrixdb \
  -s \
  -P workloads/workload_c \
  -p veltrixdb.host=127.0.0.1 \
  -p veltrixdb.port=9000 \
  -threads 128
```

### Workload D — Read latest (95R/5 insert)

```bash
bin/ycsb run veltrixdb \
  -s \
  -P workloads/workload_d \
  -p veltrixdb.host=127.0.0.1 \
  -p veltrixdb.port=9000 \
  -threads 64
```

### Workload E — Short ranges (95 scan/5 insert)

```bash
# Load E dataset first
bin/ycsb load veltrixdb \
  -s \
  -P workloads/workload_e \
  -p veltrixdb.host=127.0.0.1 \
  -p veltrixdb.port=9000 \
  -threads 32

# Run
bin/ycsb run veltrixdb \
  -s \
  -P workloads/workload_e \
  -p veltrixdb.host=127.0.0.1 \
  -p veltrixdb.port=9000 \
  -threads 32
```

### Workload F — Read-Modify-Write (50R/50 RMW)

```bash
bin/ycsb run veltrixdb \
  -s \
  -P workloads/workload_f \
  -p veltrixdb.host=127.0.0.1 \
  -p veltrixdb.port=9000 \
  -threads 64
```

---

## Step 9 — Reading the output

YCSB prints per-operation stats at the end:

```
[OVERALL], Throughput(ops/sec), 487302.4
[READ],    AverageLatency(us),  198.3
[READ],    MinLatency(us),      42
[READ],    MaxLatency(us),      18415
[READ],    95thPercentileLatency(us), 412
[READ],    99thPercentileLatency(us), 1023
[UPDATE],  AverageLatency(us),  234.7
[UPDATE],  95thPercentileLatency(us), 589
[UPDATE],  99thPercentileLatency(us), 1204
```

Key metrics to capture for public comparison:

| Metric | Flag in output |
|--------|---------------|
| Overall throughput | `[OVERALL], Throughput(ops/sec)` |
| Read P95 / P99 | `[READ], 95thPercentileLatency(us)` |
| Write P95 / P99 | `[UPDATE], 95thPercentileLatency(us)` |

---

## All workloads in one script

```bash
#!/usr/bin/env bash
set -euo pipefail

YCSB=./bin/ycsb
HOST=127.0.0.1
PORT=9000
THREADS=64
RESULTS_DIR=./results/$(date +%Y%m%d-%H%M%S)
mkdir -p "$RESULTS_DIR"

COMMON="-p veltrixdb.host=$HOST -p veltrixdb.port=$PORT -threads $THREADS"

echo "==> Loading dataset..."
$YCSB load veltrixdb -s -P workloads/workload_a $COMMON \
  | tee "$RESULTS_DIR/load.txt"

for wl in a b c d f; do
  echo "==> Workload $wl"
  $YCSB run veltrixdb -s -P workloads/workload_$wl $COMMON \
    | tee "$RESULTS_DIR/workload_${wl}.txt"
done

echo "==> Workload e (separate load)"
$YCSB load veltrixdb -s -P workloads/workload_e $COMMON \
  | tee "$RESULTS_DIR/load_e.txt"
$YCSB run veltrixdb -s -P workloads/workload_e \
  -p veltrixdb.host=$HOST -p veltrixdb.port=$PORT -threads 32 \
  | tee "$RESULTS_DIR/workload_e.txt"

echo "Done. Results in $RESULTS_DIR"
```

Save as `run_all_ycsb.sh`, `chmod +x` it, and run from the YCSB root.

---

## Tuning for higher numbers

### Thread count
Start at `threads=64`. Increase until throughput plateaus — that is your saturation point. For Workload C (read-only) go higher (`128` or `256`).

### VeltrixDB server flags that matter for benchmarks

```bash
go run ./cmd/server \
  --port 9000 \
  --cache 8192 \        # increase cache to fit working set (MB)
  --data-dir /nvme/vdb  # put data on NVMe, not HDD
```

### Record size
The workload files use `fieldcount=1 fieldlength=1000` (1 KB records). To test with smaller or larger values:

```bash
bin/ycsb run veltrixdb -s -P workloads/workload_a \
  -p fieldcount=1 \
  -p fieldlength=256 \    # 256-byte records
  -p veltrixdb.host=127.0.0.1 \
  -p veltrixdb.port=9000 \
  -threads 64
```

### Key distribution
- `zipfian` — hot key skew (default, realistic)
- `uniform` — every key equally likely (stress tests GC + no cache benefit)
- `latest` — skew toward newest keys (workload D)

Override in the run command:
```bash
-p requestdistribution=uniform
```

---

## Comparing with Redis

Load Redis first, then run YCSB with the Redis binding for an apples-to-apples comparison:

```bash
# Redis — same workload, same thread count
bin/ycsb run redis -s -P workloads/workload_a \
  -p redis.host=127.0.0.1 \
  -p redis.port=6379 \
  -threads 64
```

Run VeltrixDB immediately after on the same machine to eliminate hardware variance.

---

## Monitoring during the run

While YCSB is running, watch VeltrixDB's Prometheus metrics in real time:

```bash
# In a separate terminal — shows ops/s, cache hit rate, GC state every 2s
watch -n2 "curl -s http://127.0.0.1:2112/metrics | grep -E 'veltrixdb_(reads|writes|cache|gc)_total'"
```

Or open the built-in web dashboard:
```
http://127.0.0.1:2112/admin/ui
```

---

## Troubleshooting

**`ClassNotFoundException: site.ycsb.db.VeltrixDBYCSBClient`**
The fat JAR is not in `veltrixdb/lib/`. Re-check Step 5.

**`Cannot connect to VeltrixDB`**
VeltrixDB server is not running or the port/host property is wrong.

**`VeltrixDB: cannot install com.veltrixdb:veltrixdb-client:1.0.0`**
Run Step 2 — install the Java client into your local Maven repo first.

**Low throughput on first run**
The dataset is not warm. Run Workload C (100% read) once to warm the LIRS cache, then re-run your target workload.

**`BUILD FAILURE` in Maven with `YCSB core not found`**
YCSB core (`site.ycsb:core:0.17.0`) is on Maven Central — ensure you have an internet connection or a local Maven Central mirror configured.

---

## File locations reference

```
veltrixdb-benchmark/
├── YCSB_GUIDE.md                          ← this file
└── ycsb-binding/
    ├── pom.xml                            ← Maven build
    ├── src/main/java/site/ycsb/db/
    │   └── VeltrixDBYCSBClient.java       ← YCSB DB binding
    └── workloads/
        ├── workload_a                     ← 50R/50W
        ├── workload_b                     ← 95R/5W
        ├── workload_c                     ← 100R
        ├── workload_d                     ← 95R/5 insert (latest)
        ├── workload_e                     ← 95 scan/5 insert
        └── workload_f                     ← 50R/50 RMW
```
