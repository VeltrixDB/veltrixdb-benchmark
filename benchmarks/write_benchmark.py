#!/usr/bin/env python3
"""
VeltrixDB write benchmark.

Measures throughput and latency for single-key PUT and batched MPUT operations.
Supports configurable concurrency, value sizes, batch sizes, and duration.

Usage:
    python3 benchmarks/write_benchmark.py --host 127.0.0.1 --port 9000 \\
        --concurrency 16 --duration 60 --value-size 128 --batch-size 256

Output:
    - Console table with ops/s, p50/p95/p99 latency
    - results/write_<timestamp>.csv
    - results/write_<timestamp>.json
"""

import argparse
import csv
import json
import os
import random
import string
import sys
import threading
import time
from pathlib import Path
from statistics import mean
from typing import List, Optional

# Allow running directly from repo root
sys.path.insert(0, str(Path(__file__).parent))
from client import VeltrixDBPool, VeltrixDBError


# ── Latency histogram ──────────────────────────────────────────────────────────

class LatencyTracker:
    """Lock-free per-thread latency accumulator; merged at report time."""

    def __init__(self):
        self._lock = threading.Lock()
        self._samples: List[float] = []
        self._ops = 0
        self._errors = 0
        self._bytes = 0

    def record(self, latency_s: float, payload_bytes: int = 0) -> None:
        with self._lock:
            self._samples.append(latency_s * 1000)  # store as ms
            self._ops += 1
            self._bytes += payload_bytes

    def record_error(self) -> None:
        with self._lock:
            self._errors += 1

    def merge(self, other: 'LatencyTracker') -> None:
        with self._lock:
            with other._lock:
                self._samples.extend(other._samples)
                self._ops += other._ops
                self._errors += other._errors
                self._bytes += other._bytes

    def percentile(self, p: float) -> float:
        if not self._samples:
            return 0.0
        s = sorted(self._samples)
        idx = int(len(s) * p / 100)
        return s[min(idx, len(s) - 1)]

    @property
    def ops(self) -> int:
        return self._ops

    @property
    def errors(self) -> int:
        return self._errors

    @property
    def total_bytes(self) -> int:
        return self._bytes


# ── Key / value generation ─────────────────────────────────────────────────────

def make_key(i: int, prefix: str = 'bench') -> str:
    return f"{prefix}:{i:016x}"

def make_value(size: int, rng: random.Random) -> bytes:
    # Fast path: use random bytes for realistic compressibility test
    return bytes(rng.getrandbits(8) for _ in range(size))


# ── Worker ─────────────────────────────────────────────────────────────────────

def write_worker(
    worker_id: int,
    pool: VeltrixDBPool,
    tracker: LatencyTracker,
    num_keys: int,
    value_size: int,
    batch_size: int,
    stop_event: threading.Event,
    key_counter: 'threading.Semaphore',
    written_hwm: threading.local,
) -> None:
    rng = random.Random(worker_id * 31337 + 1)
    value = make_value(value_size, rng)

    while not stop_event.is_set():
        if batch_size <= 1:
            # Single PUT
            key = make_key(rng.randint(0, num_keys - 1))
            t0 = time.perf_counter()
            try:
                with pool.acquire() as c:
                    c.put(key, value)
                tracker.record(time.perf_counter() - t0, value_size)
            except VeltrixDBError:
                tracker.record_error()
        else:
            # Batched MPUT
            batch = [
                (make_key(rng.randint(0, num_keys - 1)), value)
                for _ in range(batch_size)
            ]
            t0 = time.perf_counter()
            try:
                with pool.acquire() as c:
                    errors = c.multi_put(batch)
                elapsed = time.perf_counter() - t0
                err_count = sum(1 for e in errors if e is not None)
                ok_count = len(batch) - err_count
                for _ in range(ok_count):
                    tracker.record(elapsed / max(ok_count, 1), value_size)
                for _ in range(err_count):
                    tracker.record_error()
            except VeltrixDBError:
                tracker.record_error()


# ── Reporter ───────────────────────────────────────────────────────────────────

def print_report(tracker: LatencyTracker, elapsed: float, value_size: int, batch_size: int) -> dict:
    ops = tracker.ops
    errs = tracker.errors
    ops_per_sec = ops / elapsed if elapsed > 0 else 0
    throughput_mb = (tracker.total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
    p50 = tracker.percentile(50)
    p95 = tracker.percentile(95)
    p99 = tracker.percentile(99)
    p999 = tracker.percentile(99.9)

    print()
    print("┌─────────────────────────────────────────────────┐")
    print("│              WRITE BENCHMARK RESULTS             │")
    print("├─────────────────────────────────────────────────┤")
    print(f"│  Operations        {ops:>28,}  │")
    print(f"│  Errors            {errs:>28,}  │")
    print(f"│  Duration          {elapsed:>27.2f}s  │")
    print(f"│  Throughput        {ops_per_sec:>25,.0f} ops/s  │")
    print(f"│  Data rate         {throughput_mb:>25.2f} MB/s  │")
    print(f"│  Value size        {value_size:>27}B  │")
    print(f"│  Batch size        {batch_size:>28,}  │")
    print("├─────────────────────────────────────────────────┤")
    print(f"│  Latency P50       {p50:>26.3f} ms  │")
    print(f"│  Latency P95       {p95:>26.3f} ms  │")
    print(f"│  Latency P99       {p99:>26.3f} ms  │")
    print(f"│  Latency P99.9     {p999:>26.3f} ms  │")
    print("└─────────────────────────────────────────────────┘")
    print()

    return {
        'mode': 'write',
        'ops': ops,
        'errors': errs,
        'duration_s': round(elapsed, 3),
        'ops_per_sec': round(ops_per_sec, 1),
        'throughput_mb_per_s': round(throughput_mb, 2),
        'value_size_bytes': value_size,
        'batch_size': batch_size,
        'p50_ms': round(p50, 3),
        'p95_ms': round(p95, 3),
        'p99_ms': round(p99, 3),
        'p999_ms': round(p999, 3),
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description='VeltrixDB write benchmark')
    parser.add_argument('--host',        default='127.0.0.1')
    parser.add_argument('--port',        type=int, default=9000)
    parser.add_argument('--concurrency', type=int, default=16)
    parser.add_argument('--duration',    type=int, default=60, help='Benchmark duration in seconds')
    parser.add_argument('--warmup',      type=int, default=10, help='Warmup duration in seconds')
    parser.add_argument('--num-keys',    type=int, default=1_000_000)
    parser.add_argument('--value-size',  type=int, default=128)
    parser.add_argument('--batch-size',  type=int, default=1,
                        help='Entries per MPUT call. 1 = single PUT.')
    parser.add_argument('--output-dir',  default='results')
    parser.add_argument('--report-every', type=int, default=10,
                        help='Print interim report every N seconds')
    args = parser.parse_args()

    pool_size = max(args.concurrency, 4)
    pool = VeltrixDBPool(
        host=args.host,
        port=args.port,
        pool_size=pool_size,
        timeout=30.0,
    )

    # Verify connectivity
    try:
        pool.execute(lambda c: c.ping())
        print(f"Connected to VeltrixDB at {args.host}:{args.port}")
    except Exception as e:
        print(f"Failed to connect: {e}", file=sys.stderr)
        sys.exit(1)

    stop_event = threading.Event()
    global_tracker = LatencyTracker()
    key_counter = threading.Semaphore(0)
    written_hwm = threading.local()

    # ── Warmup ────────────────────────────────────────────────────────────────
    if args.warmup > 0:
        print(f"Warming up for {args.warmup}s...")
        warmup_stop = threading.Event()
        warmup_tracker = LatencyTracker()
        workers = []
        for i in range(args.concurrency):
            t = threading.Thread(
                target=write_worker,
                args=(i, pool, warmup_tracker, args.num_keys, args.value_size,
                      args.batch_size, warmup_stop, key_counter, written_hwm),
                daemon=True,
            )
            t.start()
            workers.append(t)
        time.sleep(args.warmup)
        warmup_stop.set()
        for t in workers:
            t.join(timeout=5)
        print(f"  Warmup complete: {warmup_tracker.ops:,} ops")

    # ── Benchmark ─────────────────────────────────────────────────────────────
    print(f"\nRunning write benchmark for {args.duration}s "
          f"({args.concurrency} workers, batch={args.batch_size}, val={args.value_size}B)...")

    workers = []
    thread_trackers = []
    for i in range(args.concurrency):
        t_tracker = LatencyTracker()
        thread_trackers.append(t_tracker)
        t = threading.Thread(
            target=write_worker,
            args=(i, pool, t_tracker, args.num_keys, args.value_size,
                  args.batch_size, stop_event, key_counter, written_hwm),
            daemon=True,
        )
        t.start()
        workers.append(t)

    t_start = time.perf_counter()
    last_report = t_start
    last_ops = 0

    try:
        while True:
            now = time.perf_counter()
            if now - t_start >= args.duration:
                break
            if now - last_report >= args.report_every:
                current_ops = sum(t.ops for t in thread_trackers)
                interval_ops = current_ops - last_ops
                interval_s = now - last_report
                print(f"  [{now - t_start:6.1f}s] {current_ops:>10,} ops  "
                      f"{interval_ops / interval_s:>8,.0f} ops/s")
                last_report = now
                last_ops = current_ops
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nInterrupted.")

    stop_event.set()
    for t in workers:
        t.join(timeout=5)

    elapsed = time.perf_counter() - t_start

    # Merge all thread trackers
    for t_tracker in thread_trackers:
        global_tracker.merge(t_tracker)

    result = print_report(global_tracker, elapsed, args.value_size, args.batch_size)

    # ── Export results ─────────────────────────────────────────────────────────
    os.makedirs(args.output_dir, exist_ok=True)
    ts = time.strftime('%Y%m%d-%H%M%S')

    json_path = os.path.join(args.output_dir, f'write_{ts}.json')
    with open(json_path, 'w') as f:
        json.dump(result, f, indent=2)

    csv_path = os.path.join(args.output_dir, f'write_{ts}.csv')
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=result.keys())
        w.writeheader()
        w.writerow(result)

    print(f"Results written to: {json_path}")
    pool.close()


if __name__ == '__main__':
    main()
