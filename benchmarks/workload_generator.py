#!/usr/bin/env python3
"""
VeltrixDB workload generator — YCSB-inspired mixed workloads.

Supports YCSB workloads A–F plus custom profiles loaded from YAML files.
Collects combined read + write metrics and exports a unified summary JSON.

YCSB workload definitions:
  A — 50% reads, 50% writes  (update heavy)
  B — 95% reads, 5% writes   (read mostly)
  C — 100% reads              (read only)
  D — 95% reads, 5% inserts  (read latest — Zipfian latest distribution)
  E — 95% scans, 5% inserts  (short ranges — not applicable here, mapped to B)
  F — 50% reads, 50% RMW     (read-modify-write — uses GET+PUT)

Usage:
    python3 benchmarks/workload_generator.py \\
        --host 127.0.0.1 --port 9000 \\
        --mode mixed --read-ratio 0.7 \\
        --concurrency 16 --duration 60 \\
        --num-keys 1000000 --value-size 128 --batch-size 256 \\
        --metrics-url http://127.0.0.1:2112/metrics \\
        --output-dir results/

    # Load from benchmark profile YAML:
    python3 benchmarks/workload_generator.py \\
        --profile config/benchmark-profiles/mixed.yaml
"""

import argparse
import csv
import json
import os
import random
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))
from client import VeltrixDBPool, VeltrixDBError


# ── Metric collection ──────────────────────────────────────────────────────────

def scrape_metrics(url: str) -> Dict[str, float]:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = resp.read().decode('utf-8')
    except Exception:
        return {}
    result: Dict[str, float] = {}
    for line in body.splitlines():
        if line.startswith('#') or not line.strip():
            continue
        parts = line.rsplit(' ', 1)
        if len(parts) == 2:
            try:
                result[parts[0].split('{')[0]] = float(parts[1])
            except ValueError:
                pass
    return result


# ── Data types ─────────────────────────────────────────────────────────────────

class Stats:
    def __init__(self, mode: str):
        self.mode = mode
        self._lock = threading.Lock()
        self.ops = 0
        self.errors = 0
        self.bytes = 0
        self._latencies: List[float] = []

    def record(self, latency_s: float, payload: int = 0):
        with self._lock:
            self._latencies.append(latency_s * 1000)
            self.ops += 1
            self.bytes += payload

    def error(self):
        with self._lock:
            self.errors += 1

    def merge(self, other: 'Stats'):
        with self._lock:
            with other._lock:
                self._latencies.extend(other._latencies)
                self.ops += other.ops
                self.errors += other.errors
                self.bytes += other.bytes

    def pct(self, p: float) -> float:
        if not self._latencies:
            return 0.0
        s = sorted(self._latencies)
        return s[min(int(len(s) * p / 100), len(s) - 1)]

    def to_dict(self, elapsed: float) -> dict:
        return {
            'mode': self.mode,
            'ops': self.ops,
            'errors': self.errors,
            'duration_s': round(elapsed, 3),
            'ops_per_sec': round(self.ops / max(elapsed, 1e-9), 1),
            'throughput_mb_per_s': round(self.bytes / (1024**2) / max(elapsed, 1e-9), 2),
            'p50_ms':  round(self.pct(50), 3),
            'p95_ms':  round(self.pct(95), 3),
            'p99_ms':  round(self.pct(99), 3),
            'p999_ms': round(self.pct(99.9), 3),
        }


# ── Key / value ────────────────────────────────────────────────────────────────

def key(i: int) -> str:
    return f"wl:{i:016x}"

def val(size: int, seed: int = 0) -> bytes:
    r = random.Random(seed)
    return bytes(r.getrandbits(8) for _ in range(size))


# ── Workers ────────────────────────────────────────────────────────────────────

def mixed_worker(
    wid: int,
    pool: VeltrixDBPool,
    r_stats: Stats,
    w_stats: Stats,
    num_keys: int,
    value_size: int,
    batch_size: int,
    read_ratio: float,
    stop: threading.Event,
) -> None:
    rng = random.Random(wid * 137 + 17)
    value = val(value_size, wid)

    while not stop.is_set():
        if batch_size <= 1:
            if rng.random() < read_ratio:
                k = key(rng.randint(0, num_keys - 1))
                t0 = time.perf_counter()
                try:
                    with pool.acquire() as c:
                        v = c.get(k)
                    r_stats.record(time.perf_counter() - t0, len(v) if v else 0)
                except VeltrixDBError:
                    r_stats.error()
            else:
                k = key(rng.randint(0, num_keys - 1))
                t0 = time.perf_counter()
                try:
                    with pool.acquire() as c:
                        c.put(k, value)
                    w_stats.record(time.perf_counter() - t0, value_size)
                except VeltrixDBError:
                    w_stats.error()
        else:
            # Decide batch type
            if rng.random() < read_ratio:
                keys = [key(rng.randint(0, num_keys - 1)) for _ in range(batch_size)]
                t0 = time.perf_counter()
                try:
                    with pool.acquire() as c:
                        results = c.multi_get(keys)
                    elapsed = time.perf_counter() - t0
                    per = elapsed / max(len(keys), 1)
                    for v in results:
                        r_stats.record(per, len(v) if v else 0)
                except VeltrixDBError:
                    r_stats.error()
            else:
                batch = [(key(rng.randint(0, num_keys - 1)), value) for _ in range(batch_size)]
                t0 = time.perf_counter()
                try:
                    with pool.acquire() as c:
                        errs = c.multi_put(batch)
                    elapsed = time.perf_counter() - t0
                    per = elapsed / max(len(batch), 1)
                    for e in errs:
                        if e is None:
                            w_stats.record(per, value_size)
                        else:
                            w_stats.error()
                except VeltrixDBError:
                    w_stats.error()


def rmw_worker(
    wid: int,
    pool: VeltrixDBPool,
    r_stats: Stats,
    w_stats: Stats,
    num_keys: int,
    value_size: int,
    stop: threading.Event,
) -> None:
    rng = random.Random(wid * 421 + 7)
    new_value = val(value_size, wid)

    while not stop.is_set():
        k = key(rng.randint(0, num_keys - 1))
        # Read phase
        t0 = time.perf_counter()
        try:
            with pool.acquire() as c:
                v = c.get(k)
            r_stats.record(time.perf_counter() - t0, len(v) if v else 0)
        except VeltrixDBError:
            r_stats.error()
            continue
        # Modify + write phase
        t0 = time.perf_counter()
        try:
            with pool.acquire() as c:
                c.put(k, new_value)
            w_stats.record(time.perf_counter() - t0, value_size)
        except VeltrixDBError:
            w_stats.error()


# ── Profile loader ─────────────────────────────────────────────────────────────

def load_yaml_profile(path: str) -> dict:
    result = {}
    section = None
    import re
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            if re.match(r'^[a-zA-Z]', line):
                section = line.rstrip(':')
                continue
            m = re.match(r'^\s{2,}(\w[\w_-]*):\s*(.*)', line)
            if m and section in ('benchmark', 'veltrixdb'):
                k = m.group(1).replace('-', '_')
                v = m.group(2).strip().strip('"').strip("'").split('#')[0].strip()
                if v:
                    result[f"{section}__{k}"] = v
    return result


# ── YCSB presets ──────────────────────────────────────────────────────────────

YCSB_PRESETS = {
    'A': {'read_ratio': 0.50, 'mode': 'mixed', 'description': '50% R / 50% W  (update-heavy)'},
    'B': {'read_ratio': 0.95, 'mode': 'mixed', 'description': '95% R / 5% W   (read-mostly)'},
    'C': {'read_ratio': 1.00, 'mode': 'read',  'description': '100% R          (read-only)'},
    'D': {'read_ratio': 0.95, 'mode': 'mixed', 'description': '95% R / 5% ins  (read-latest)'},
    'F': {'read_ratio': 0.50, 'mode': 'rmw',   'description': '50% R / 50% RMW (read-modify-write)'},
}


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description='VeltrixDB workload generator')
    parser.add_argument('--host',          default='127.0.0.1')
    parser.add_argument('--port',          type=int, default=9000)
    parser.add_argument('--metrics-url',   default='http://127.0.0.1:2112/metrics')
    parser.add_argument('--mode',          choices=['write', 'read', 'mixed', 'rmw'],
                        default='mixed')
    parser.add_argument('--ycsb',          choices=list(YCSB_PRESETS.keys()),
                        help='Use a YCSB workload preset (overrides --mode / --read-ratio)')
    parser.add_argument('--profile',       help='Path to benchmark profile YAML')
    parser.add_argument('--concurrency',   type=int, default=16)
    parser.add_argument('--duration',      type=int, default=60)
    parser.add_argument('--warmup',        type=int, default=10)
    parser.add_argument('--num-keys',      type=int, default=1_000_000)
    parser.add_argument('--value-size',    type=int, default=128)
    parser.add_argument('--batch-size',    type=int, default=1)
    parser.add_argument('--read-ratio',    type=float, default=0.7)
    parser.add_argument('--output-dir',    default='results')
    parser.add_argument('--report-every',  type=int, default=10)
    args = parser.parse_args()

    # YAML profile overrides defaults
    if args.profile:
        profile = load_yaml_profile(args.profile)
        for k, v in profile.items():
            section, name = k.split('__', 1)
            if section == 'benchmark':
                attr = name.replace('-', '_')
                if hasattr(args, attr):
                    current = getattr(args, attr)
                    try:
                        setattr(args, attr, type(current)(v))
                    except (ValueError, TypeError):
                        setattr(args, attr, v)

    # YCSB preset overrides mode + read-ratio
    if args.ycsb:
        preset = YCSB_PRESETS[args.ycsb]
        args.mode = preset['mode']
        args.read_ratio = preset['read_ratio']
        print(f"YCSB Workload {args.ycsb}: {preset['description']}")

    pool = VeltrixDBPool(
        host=args.host, port=args.port,
        pool_size=max(args.concurrency, 8),
        timeout=30.0,
    )
    try:
        pool.execute(lambda c: c.ping())
        print(f"Connected to VeltrixDB at {args.host}:{args.port}")
    except Exception as e:
        print(f"Failed to connect: {e}", file=sys.stderr)
        sys.exit(1)

    # Snapshot metrics at start
    m_start = scrape_metrics(args.metrics_url)

    # ── Warmup ─────────────────────────────────────────────────────────────────
    if args.warmup > 0:
        print(f"Warming up for {args.warmup}s...")
        warmup_stop = threading.Event()
        ws = Stats('write')
        rs = Stats('read')
        workers = [
            threading.Thread(
                target=mixed_worker,
                args=(i, pool, rs, ws, args.num_keys, args.value_size,
                      args.batch_size, args.read_ratio, warmup_stop),
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
        print(f"  Warmup: {ws.ops + rs.ops:,} ops")

    # ── Benchmark ──────────────────────────────────────────────────────────────
    mode_label = f"{args.mode} (R={args.read_ratio:.0%})" if args.mode == 'mixed' else args.mode
    print(f"\nRunning {mode_label} benchmark: {args.duration}s, "
          f"{args.concurrency} workers, batch={args.batch_size}, "
          f"keys={args.num_keys:,}, val={args.value_size}B")

    stop = threading.Event()
    r_trackers, w_trackers = [], []
    workers = []

    for i in range(args.concurrency):
        rs = Stats('read')
        ws = Stats('write')
        r_trackers.append(rs)
        w_trackers.append(ws)

        if args.mode == 'rmw':
            t = threading.Thread(
                target=rmw_worker,
                args=(i, pool, rs, ws, args.num_keys, args.value_size, stop),
                daemon=True,
            )
        else:
            rr = 1.0 if args.mode == 'read' else (0.0 if args.mode == 'write' else args.read_ratio)
            t = threading.Thread(
                target=mixed_worker,
                args=(i, pool, rs, ws, args.num_keys, args.value_size,
                      args.batch_size, rr, stop),
                daemon=True,
            )
        t.start()
        workers.append(t)

    t_start = time.perf_counter()
    last_report = t_start
    last_total = 0

    try:
        while True:
            now = time.perf_counter()
            if now - t_start >= args.duration:
                break
            if now - last_report >= args.report_every:
                total = sum(s.ops for s in r_trackers) + sum(s.ops for s in w_trackers)
                interval = total - last_total
                rate = interval / (now - last_report)
                rops = sum(s.ops for s in r_trackers)
                wops = sum(s.ops for s in w_trackers)
                print(f"  [{now - t_start:6.1f}s] total={total:>10,}  "
                      f"{rate:>8,.0f} ops/s  R={rops:,}  W={wops:,}")
                last_report = now
                last_total = total
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nInterrupted.")

    stop.set()
    for t in workers:
        t.join(timeout=5)

    elapsed = time.perf_counter() - t_start
    m_end = scrape_metrics(args.metrics_url)

    # Merge per-thread stats
    global_r = Stats('read')
    global_w = Stats('write')
    for rs in r_trackers:
        global_r.merge(rs)
    for ws in w_trackers:
        global_w.merge(ws)

    total_ops = global_r.ops + global_w.ops
    total_ops_per_sec = total_ops / max(elapsed, 1e-9)

    print()
    print("┌─────────────────────────────────────────────────────────────────┐")
    print("│                    WORKLOAD GENERATOR RESULTS                    │")
    print("├─────────────────────────────────────────────────────────────────┤")
    print(f"│  Mode            {mode_label:<48}  │")
    print(f"│  Total ops       {total_ops:>48,}  │")
    print(f"│  Total ops/s     {total_ops_per_sec:>46,.0f}  │")
    print(f"│  Duration        {elapsed:>46.2f}s  │")
    print("├──────────────────────────────┬──────────────────────────────────┤")
    print(f"│  {'READ':^28}  │  {'WRITE':^30}  │")
    print("├──────────────────────────────┼──────────────────────────────────┤")
    rd = global_r.to_dict(elapsed)
    wd = global_w.to_dict(elapsed)
    for metric in ('ops', 'ops_per_sec', 'p50_ms', 'p95_ms', 'p99_ms'):
        rv = rd.get(metric, 0)
        wv = wd.get(metric, 0)
        label = metric.replace('_', ' ').replace('ms', '(ms)').title()
        print(f"│  {label:<10} {rv:>16.1f}  │  {label:<10} {wv:>18.1f}  │")
    print("└──────────────────────────────┴──────────────────────────────────┘")

    # Prometheus metrics delta
    if m_start and m_end:
        print("\nVeltrixDB Server Metrics (delta):")
        server_metrics = [
            ('veltrixdb_writes_total',                     'Total writes'),
            ('veltrixdb_reads_total',                      'Total reads'),
            ('veltrixdb_vlog_gc_runs_total',               'GC runs'),
            ('veltrixdb_vlog_gc_emergency_runs_total',     'Emergency GC'),
            ('veltrixdb_vlog_gc_bytes_reclaimed_total',    'Bytes reclaimed'),
            ('veltrixdb_storage_write_admission_throttles_total', 'Write throttles'),
            ('veltrixdb_cache_hits_total',                 'Cache hits'),
            ('veltrixdb_cache_misses_total',               'Cache misses'),
        ]
        for metric, label in server_metrics:
            delta = m_end.get(metric, 0) - m_start.get(metric, 0)
            if delta > 0 or metric.endswith('_ratio') or metric.endswith('_bytes'):
                if 'bytes' in metric and delta > 1024:
                    print(f"  {label:<36} {delta / (1024**3):>10.3f} GB")
                else:
                    print(f"  {label:<36} {delta:>10,.0f}")

    # Cache hit rate from server metrics
    ch = m_end.get('veltrixdb_cache_hits_total', 0) - m_start.get('veltrixdb_cache_hits_total', 0)
    cm = m_end.get('veltrixdb_cache_misses_total', 0) - m_start.get('veltrixdb_cache_misses_total', 0)
    if ch + cm > 0:
        print(f"\n  Cache hit rate (server): {ch / (ch + cm):.1%}")
    print()

    # ── Export ─────────────────────────────────────────────────────────────────
    os.makedirs(args.output_dir, exist_ok=True)
    ts = time.strftime('%Y%m%d-%H%M%S')

    summary = {
        'timestamp': ts,
        'mode': args.mode,
        'ycsb_workload': args.ycsb,
        'concurrency': args.concurrency,
        'duration_s': round(elapsed, 3),
        'total_ops': total_ops,
        'total_ops_per_sec': round(total_ops_per_sec, 1),
        'read': rd,
        'write': wd,
        'server_metrics_delta': {
            m: m_end.get(m, 0) - m_start.get(m, 0)
            for m in [k for k, _ in server_metrics]
            if m in m_end
        },
    }

    summary_path = os.path.join(args.output_dir, 'summary.json')
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)

    detail_path = os.path.join(args.output_dir, f'workload_{ts}.json')
    with open(detail_path, 'w') as f:
        json.dump(summary, f, indent=2)

    csv_path = os.path.join(args.output_dir, f'workload_{ts}.csv')
    row = {
        'timestamp': ts, 'mode': args.mode,
        'concurrency': args.concurrency, 'duration_s': round(elapsed, 3),
        'total_ops': total_ops, 'total_ops_per_sec': round(total_ops_per_sec, 1),
        **{f'read_{k}': v for k, v in rd.items()},
        **{f'write_{k}': v for k, v in wd.items()},
    }
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        w.writeheader()
        w.writerow(row)

    print(f"Summary: {summary_path}")
    print(f"Detail:  {detail_path}")
    pool.close()


if __name__ == '__main__':
    main()
