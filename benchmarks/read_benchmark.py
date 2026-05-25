#!/usr/bin/env python3
"""
VeltrixDB read benchmark.

Measures read throughput, latency distribution, and cache hit efficiency.
Supports single GET, batched MGET, warm-cache and cold-cache modes.

Usage:
    # First populate with write_benchmark.py, then:
    python3 benchmarks/read_benchmark.py --host 127.0.0.1 --port 9000 \\
        --concurrency 32 --duration 60 --num-keys 100000

    # Pre-populate + read in one command:
    python3 benchmarks/read_benchmark.py --populate --num-keys 100000 --value-size 128
"""

import argparse
import csv
import json
import os
import random
import sys
import threading
import time
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).parent))
from client import VeltrixDBPool, VeltrixDBClient, VeltrixDBError


# ── Tracker ────────────────────────────────────────────────────────────────────

class ReadTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._latencies: List[float] = []
        self._hits = 0
        self._misses = 0
        self._errors = 0
        self._bytes = 0

    def record(self, latency_s: float, found: bool, payload_bytes: int = 0):
        with self._lock:
            self._latencies.append(latency_s * 1000)
            if found:
                self._hits += 1
                self._bytes += payload_bytes
            else:
                self._misses += 1

    def record_error(self):
        with self._lock:
            self._errors += 1

    def merge(self, other: 'ReadTracker'):
        with self._lock:
            with other._lock:
                self._latencies.extend(other._latencies)
                self._hits += other._hits
                self._misses += other._misses
                self._errors += other._errors
                self._bytes += other._bytes

    def percentile(self, p: float) -> float:
        if not self._latencies:
            return 0.0
        s = sorted(self._latencies)
        idx = int(len(s) * p / 100)
        return s[min(idx, len(s) - 1)]

    @property
    def ops(self):
        return self._hits + self._misses

    @property
    def hit_rate(self) -> float:
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0


# ── Helpers ────────────────────────────────────────────────────────────────────

def make_key(i: int, prefix: str = 'bench') -> str:
    return f"{prefix}:{i:016x}"

def make_value(size: int) -> bytes:
    return b'x' * size


# ── Workers ────────────────────────────────────────────────────────────────────

def populate_worker(
    worker_id: int,
    pool: VeltrixDBPool,
    num_keys: int,
    value_size: int,
    batch_size: int,
    key_queue: 'threading.Semaphore',
    done_event: threading.Event,
) -> None:
    value = make_value(value_size)
    rng = random.Random(worker_id)
    while True:
        start = rng.randint(0, num_keys - 1)
        end = min(start + batch_size, num_keys)
        batch = [(make_key(i), value) for i in range(start, end)]
        try:
            with pool.acquire() as c:
                c.multi_put(batch)
        except VeltrixDBError:
            pass
        if done_event.is_set():
            break


def read_worker(
    worker_id: int,
    pool: VeltrixDBPool,
    tracker: ReadTracker,
    num_keys: int,
    batch_size: int,
    stop_event: threading.Event,
    access_pattern: str,
) -> None:
    rng = random.Random(worker_id * 99991 + 7)

    while not stop_event.is_set():
        if batch_size <= 1:
            if access_pattern == 'sequential':
                key_idx = rng.randint(0, num_keys - 1)
            elif access_pattern == 'hotspot':
                # 80% reads on top 20% of keys
                if rng.random() < 0.8:
                    key_idx = rng.randint(0, int(num_keys * 0.2))
                else:
                    key_idx = rng.randint(0, num_keys - 1)
            else:  # random
                key_idx = rng.randint(0, num_keys - 1)

            key = make_key(key_idx)
            t0 = time.perf_counter()
            try:
                with pool.acquire() as c:
                    val = c.get(key)
                elapsed = time.perf_counter() - t0
                tracker.record(elapsed, val is not None, len(val) if val else 0)
            except VeltrixDBError:
                tracker.record_error()
        else:
            keys = [make_key(rng.randint(0, num_keys - 1)) for _ in range(batch_size)]
            t0 = time.perf_counter()
            try:
                with pool.acquire() as c:
                    results = c.multi_get(keys)
                elapsed = time.perf_counter() - t0
                per_key = elapsed / max(len(keys), 1)
                for val in results:
                    tracker.record(per_key, val is not None, len(val) if val else 0)
            except VeltrixDBError:
                tracker.record_error()


# ── Report ─────────────────────────────────────────────────────────────────────

def print_report(tracker: ReadTracker, elapsed: float, batch_size: int) -> dict:
    ops = tracker.ops
    errs = tracker._errors
    ops_per_sec = ops / elapsed if elapsed > 0 else 0
    throughput_mb = (tracker._bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
    p50  = tracker.percentile(50)
    p95  = tracker.percentile(95)
    p99  = tracker.percentile(99)
    p999 = tracker.percentile(99.9)
    hit_rate = tracker.hit_rate * 100

    print()
    print("┌─────────────────────────────────────────────────┐")
    print("│               READ BENCHMARK RESULTS             │")
    print("├─────────────────────────────────────────────────┤")
    print(f"│  Operations        {ops:>28,}  │")
    print(f"│  Errors            {errs:>28,}  │")
    print(f"│  Cache hit rate    {hit_rate:>27.1f}%  │")
    print(f"│  Duration          {elapsed:>27.2f}s  │")
    print(f"│  Throughput        {ops_per_sec:>25,.0f} ops/s  │")
    print(f"│  Data rate         {throughput_mb:>25.2f} MB/s  │")
    print(f"│  Batch size        {batch_size:>28,}  │")
    print("├─────────────────────────────────────────────────┤")
    print(f"│  Latency P50       {p50:>26.3f} ms  │")
    print(f"│  Latency P95       {p95:>26.3f} ms  │")
    print(f"│  Latency P99       {p99:>26.3f} ms  │")
    print(f"│  Latency P99.9     {p999:>26.3f} ms  │")
    print("└─────────────────────────────────────────────────┘")
    print()

    return {
        'mode': 'read',
        'ops': ops,
        'errors': errs,
        'duration_s': round(elapsed, 3),
        'ops_per_sec': round(ops_per_sec, 1),
        'throughput_mb_per_s': round(throughput_mb, 2),
        'cache_hit_rate': round(hit_rate, 2),
        'batch_size': batch_size,
        'p50_ms': round(p50, 3),
        'p95_ms': round(p95, 3),
        'p99_ms': round(p99, 3),
        'p999_ms': round(p999, 3),
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description='VeltrixDB read benchmark')
    parser.add_argument('--host',           default='127.0.0.1')
    parser.add_argument('--port',           type=int, default=9000)
    parser.add_argument('--concurrency',    type=int, default=32)
    parser.add_argument('--duration',       type=int, default=60)
    parser.add_argument('--num-keys',       type=int, default=1_000_000)
    parser.add_argument('--value-size',     type=int, default=128,
                        help='Value size used during --populate')
    parser.add_argument('--batch-size',     type=int, default=1,
                        help='Keys per MGET call. 1 = single GET.')
    parser.add_argument('--access-pattern', choices=['random', 'sequential', 'hotspot'],
                        default='random')
    parser.add_argument('--populate',       action='store_true',
                        help='Pre-populate keys before benchmarking')
    parser.add_argument('--populate-batch', type=int, default=256)
    parser.add_argument('--warmup',         type=int, default=10)
    parser.add_argument('--output-dir',     default='results')
    parser.add_argument('--report-every',   type=int, default=10)
    args = parser.parse_args()

    pool_size = max(args.concurrency, 8)
    pool = VeltrixDBPool(host=args.host, port=args.port, pool_size=pool_size, timeout=30.0)

    try:
        pool.execute(lambda c: c.ping())
        print(f"Connected to VeltrixDB at {args.host}:{args.port}")
    except Exception as e:
        print(f"Failed to connect: {e}", file=sys.stderr)
        sys.exit(1)

    # ── Optional pre-populate ──────────────────────────────────────────────────
    if args.populate:
        print(f"Pre-populating {args.num_keys:,} keys ({args.value_size}B values)...")
        value = make_value(args.value_size)
        batch_size = args.populate_batch
        batches_done = 0
        total_batches = (args.num_keys + batch_size - 1) // batch_size

        for start in range(0, args.num_keys, batch_size):
            end = min(start + batch_size, args.num_keys)
            batch = [(make_key(i), value) for i in range(start, end)]
            try:
                pool.execute(lambda c, b=batch: c.multi_put(b))
            except VeltrixDBError as e:
                print(f"  Warning: populate batch failed: {e}", file=sys.stderr)
            batches_done += 1
            if batches_done % 100 == 0:
                pct = batches_done / total_batches * 100
                print(f"  {pct:5.1f}% ({batches_done * batch_size:,} keys)", end='\r')

        print(f"\n  Population complete: {args.num_keys:,} keys")

    # ── Warmup ─────────────────────────────────────────────────────────────────
    if args.warmup > 0:
        print(f"Warming up cache for {args.warmup}s...")
        warmup_stop = threading.Event()
        warmup_tracker = ReadTracker()
        workers = [
            threading.Thread(
                target=read_worker,
                args=(i, pool, warmup_tracker, args.num_keys, args.batch_size,
                      warmup_stop, args.access_pattern),
                daemon=True,
            )
            for i in range(args.concurrency)
        ]
        for t in workers:
            t.start()
        time.sleep(args.warmup)
        warmup_stop.set()
        for t in workers:
            t.join(timeout=5)
        print(f"  Warmup complete: {warmup_tracker.ops:,} ops, "
              f"hit rate {warmup_tracker.hit_rate:.1%}")

    # ── Benchmark ──────────────────────────────────────────────────────────────
    print(f"\nRunning read benchmark for {args.duration}s "
          f"({args.concurrency} workers, batch={args.batch_size}, pattern={args.access_pattern})...")

    thread_trackers = []
    stop_event = threading.Event()
    workers = []
    for i in range(args.concurrency):
        t_tracker = ReadTracker()
        thread_trackers.append(t_tracker)
        t = threading.Thread(
            target=read_worker,
            args=(i, pool, t_tracker, args.num_keys, args.batch_size,
                  stop_event, args.access_pattern),
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
                hit_rate = (sum(t._hits for t in thread_trackers) /
                            max(current_ops, 1) * 100)
                print(f"  [{now - t_start:6.1f}s] {current_ops:>10,} ops  "
                      f"{interval_ops / interval_s:>8,.0f} ops/s  "
                      f"hit={hit_rate:.1f}%")
                last_report = now
                last_ops = current_ops
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nInterrupted.")

    stop_event.set()
    for t in workers:
        t.join(timeout=5)

    elapsed = time.perf_counter() - t_start
    global_tracker = ReadTracker()
    for t_tracker in thread_trackers:
        global_tracker.merge(t_tracker)

    result = print_report(global_tracker, elapsed, args.batch_size)

    os.makedirs(args.output_dir, exist_ok=True)
    ts = time.strftime('%Y%m%d-%H%M%S')

    json_path = os.path.join(args.output_dir, f'read_{ts}.json')
    with open(json_path, 'w') as f:
        json.dump(result, f, indent=2)

    csv_path = os.path.join(args.output_dir, f'read_{ts}.csv')
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=result.keys())
        w.writeheader()
        w.writerow(result)

    print(f"Results written to: {json_path}")
    pool.close()


if __name__ == '__main__':
    main()
