#!/usr/bin/env python3
"""
VeltrixDB compaction / GC benchmark.

Measures VLog garbage collection efficiency by:
  1. Bulk-writing data to fill the VLog
  2. Overwriting keys repeatedly to create dead space
  3. Polling Prometheus metrics to track GC behaviour
  4. Reporting: GC runs, bytes reclaimed, emergency triggers, garbage ratio curve

Requires VeltrixDB metrics endpoint (default: http://127.0.0.1:2112/metrics).

Usage:
    python3 benchmarks/compaction_benchmark.py \\
        --host 127.0.0.1 --port 9000 \\
        --metrics-url http://127.0.0.1:2112/metrics \\
        --fill-keys 500000 --overwrite-rounds 5 \\
        --value-size 512 --batch-size 1024 \\
        --settle-time 120
"""

import argparse
import csv
import json
import os
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))
from client import VeltrixDBPool, VeltrixDBError


# ── Prometheus scraper ─────────────────────────────────────────────────────────

def scrape_metrics(url: str) -> Dict[str, float]:
    """Parse Prometheus text format and return {metric_name: value}."""
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = resp.read().decode('utf-8')
    except Exception:
        return {}

    result: Dict[str, float] = {}
    for line in body.splitlines():
        line = line.strip()
        if line.startswith('#') or not line:
            continue
        parts = line.rsplit(' ', 1)
        if len(parts) == 2:
            try:
                result[parts[0].split('{')[0]] = float(parts[1])
            except ValueError:
                pass
    return result


GC_METRICS = [
    'veltrixdb_vlog_gc_runs_total',
    'veltrixdb_vlog_gc_emergency_runs_total',
    'veltrixdb_vlog_gc_bytes_reclaimed_total',
    'veltrixdb_vlog_gc_skipped_ratio_total',
    'veltrixdb_vlog_gc_skipped_paused_total',
    'veltrixdb_vlog_garbage_ratio',
    'veltrixdb_vlog_file_bytes',
    'veltrixdb_writes_total',
    'veltrixdb_storage_write_admission_throttles_total',
]


# ── Key / value ────────────────────────────────────────────────────────────────

def make_key(i: int) -> str:
    return f"gc_test:{i:016x}"

def make_value(size: int) -> bytes:
    return b'G' * size


# ── Bulk writer ────────────────────────────────────────────────────────────────

def bulk_write(pool: VeltrixDBPool, num_keys: int, value: bytes,
               batch_size: int, label: str) -> Tuple[float, int]:
    t0 = time.perf_counter()
    errors = 0
    for start in range(0, num_keys, batch_size):
        end = min(start + batch_size, num_keys)
        batch = [(make_key(i), value) for i in range(start, end)]
        try:
            pool.execute(lambda c, b=batch: c.multi_put(b))
        except VeltrixDBError as e:
            errors += 1
        if start % (batch_size * 50) == 0:
            pct = start / num_keys * 100
            elapsed = time.perf_counter() - t0
            rate = start / max(elapsed, 0.001)
            print(f"  {label}: {pct:5.1f}% ({start:,}/{num_keys:,}) "
                  f"{rate:,.0f} keys/s", end='\r')
    elapsed = time.perf_counter() - t0
    print(f"  {label}: 100.0% ({num_keys:,}/{num_keys:,}) "
          f"{num_keys / max(elapsed, 0.001):,.0f} keys/s       ")
    return elapsed, errors


# ── GC observer ────────────────────────────────────────────────────────────────

class GCObserver(threading.Thread):
    def __init__(self, metrics_url: str, poll_interval: float = 2.0):
        super().__init__(daemon=True)
        self.metrics_url = metrics_url
        self.poll_interval = poll_interval
        self.samples: List[Dict] = []
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            m = scrape_metrics(self.metrics_url)
            if m:
                sample = {'timestamp': time.time()}
                for key in GC_METRICS:
                    sample[key] = m.get(key, 0.0)
                self.samples.append(sample)
            self._stop.wait(self.poll_interval)

    def stop(self):
        self._stop.set()


# ── Report ─────────────────────────────────────────────────────────────────────

def print_compaction_report(
    phase_results: List[Dict],
    gc_samples: List[Dict],
    settle_time: float,
) -> dict:
    if not gc_samples:
        print("Warning: no GC metric samples collected (check metrics URL)")
        return {}

    first = gc_samples[0]
    last  = gc_samples[-1]

    gc_runs_delta = last.get('veltrixdb_vlog_gc_runs_total', 0) - first.get('veltrixdb_vlog_gc_runs_total', 0)
    emergency_delta = (last.get('veltrixdb_vlog_gc_emergency_runs_total', 0) -
                       first.get('veltrixdb_vlog_gc_emergency_runs_total', 0))
    bytes_reclaimed = (last.get('veltrixdb_vlog_gc_bytes_reclaimed_total', 0) -
                       first.get('veltrixdb_vlog_gc_bytes_reclaimed_total', 0))
    final_ratio = last.get('veltrixdb_vlog_garbage_ratio', 0.0)
    final_vlog_bytes = last.get('veltrixdb_vlog_file_bytes', 0)

    # Peak garbage ratio
    peak_ratio = max((s.get('veltrixdb_vlog_garbage_ratio', 0.0) for s in gc_samples), default=0.0)

    print()
    print("┌─────────────────────────────────────────────────┐")
    print("│          COMPACTION / GC BENCHMARK RESULTS       │")
    print("├─────────────────────────────────────────────────┤")
    for ph in phase_results:
        name = ph['phase'][:20]
        wr = ph.get('write_rate', 0)
        print(f"│  {name:<20} {wr:>18,.0f} keys/s  │")
    print("├─────────────────────────────────────────────────┤")
    print(f"│  GC runs               {gc_runs_delta:>24,}  │")
    print(f"│  Emergency GC runs     {emergency_delta:>24,}  │")
    print(f"│  Bytes reclaimed       {bytes_reclaimed / (1024**3):>23.2f} GB  │")
    print(f"│  Peak garbage ratio    {peak_ratio:>24.1%}  │")
    print(f"│  Final garbage ratio   {final_ratio:>24.1%}  │")
    print(f"│  Final VLog size       {final_vlog_bytes / (1024**3):>23.2f} GB  │")
    print(f"│  Settle time           {settle_time:>23.0f}s  │")
    if emergency_delta == 0:
        print("│  Emergency GC gate     {'PASS':>24}  │")
    else:
        print("│  Emergency GC gate     {'FAIL':>24}  │")
    print("└─────────────────────────────────────────────────┘")
    print()

    return {
        'gc_runs': int(gc_runs_delta),
        'emergency_gc_runs': int(emergency_delta),
        'bytes_reclaimed_gb': round(bytes_reclaimed / (1024**3), 4),
        'peak_garbage_ratio': round(peak_ratio, 4),
        'final_garbage_ratio': round(final_ratio, 4),
        'final_vlog_bytes': int(final_vlog_bytes),
        'emergency_gc_gate': 'PASS' if emergency_delta == 0 else 'FAIL',
        'phases': phase_results,
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description='VeltrixDB compaction benchmark')
    parser.add_argument('--host',            default='127.0.0.1')
    parser.add_argument('--port',            type=int, default=9000)
    parser.add_argument('--metrics-url',     default='http://127.0.0.1:2112/metrics')
    parser.add_argument('--fill-keys',       type=int, default=500_000,
                        help='Keys to write in the initial fill phase')
    parser.add_argument('--overwrite-rounds', type=int, default=3,
                        help='Number of full overwrite passes (creates dead space)')
    parser.add_argument('--value-size',      type=int, default=512)
    parser.add_argument('--batch-size',      type=int, default=1024)
    parser.add_argument('--settle-time',     type=int, default=120,
                        help='Seconds to wait after writes for GC to settle')
    parser.add_argument('--output-dir',      default='results')
    args = parser.parse_args()

    pool = VeltrixDBPool(host=args.host, port=args.port, pool_size=8, timeout=60.0)
    try:
        pool.execute(lambda c: c.ping())
        print(f"Connected to VeltrixDB at {args.host}:{args.port}")
    except Exception as e:
        print(f"Failed to connect: {e}", file=sys.stderr)
        sys.exit(1)

    # Verify metrics endpoint
    m = scrape_metrics(args.metrics_url)
    if not m:
        print(f"Warning: could not reach metrics at {args.metrics_url}", file=sys.stderr)
    else:
        print(f"Metrics endpoint OK ({len(m)} metrics)")

    value = make_value(args.value_size)
    phase_results = []

    # Start GC observer
    observer = GCObserver(args.metrics_url, poll_interval=2.0)
    observer.start()

    # ── Phase 1: initial fill ──────────────────────────────────────────────────
    print(f"\nPhase 1: Initial fill — {args.fill_keys:,} keys × {args.value_size}B")
    elapsed, errs = bulk_write(pool, args.fill_keys, value, args.batch_size, "Fill")
    phase_results.append({
        'phase': 'initial_fill',
        'keys': args.fill_keys,
        'errors': errs,
        'write_rate': args.fill_keys / max(elapsed, 0.001),
    })

    # ── Phase 2: repeated overwrites (generates dead space) ───────────────────
    for rnd in range(1, args.overwrite_rounds + 1):
        print(f"\nPhase 2.{rnd}: Overwrite round {rnd}/{args.overwrite_rounds} "
              f"(creates dead VLog space)")
        m_before = scrape_metrics(args.metrics_url)
        elapsed, errs = bulk_write(
            pool, args.fill_keys, make_value(args.value_size + rnd),
            args.batch_size, f"Overwrite-{rnd}")
        m_after = scrape_metrics(args.metrics_url)
        gc_ratio = m_after.get('veltrixdb_vlog_garbage_ratio', 0.0)
        print(f"  VLog garbage ratio after overwrite: {gc_ratio:.1%}")
        phase_results.append({
            'phase': f'overwrite_round_{rnd}',
            'keys': args.fill_keys,
            'errors': errs,
            'write_rate': args.fill_keys / max(elapsed, 0.001),
            'gc_ratio_after': round(gc_ratio, 4),
        })

    # ── Phase 3: settle — let GC reclaim ──────────────────────────────────────
    print(f"\nPhase 3: Settle — waiting {args.settle_time}s for GC to reclaim space...")
    t_settle = time.perf_counter()
    prev_runs = scrape_metrics(args.metrics_url).get('veltrixdb_vlog_gc_runs_total', 0)
    for i in range(args.settle_time):
        time.sleep(1)
        m = scrape_metrics(args.metrics_url)
        gc_runs = m.get('veltrixdb_vlog_gc_runs_total', 0)
        gc_ratio = m.get('veltrixdb_vlog_garbage_ratio', 0.0)
        reclaimed = m.get('veltrixdb_vlog_gc_bytes_reclaimed_total', 0)
        emergency = m.get('veltrixdb_vlog_gc_emergency_runs_total', 0)
        print(f"  [{i+1:3d}s] ratio={gc_ratio:.1%}  gc_runs={gc_runs:.0f}  "
              f"reclaimed={reclaimed/(1024**3):.2f}GB  emergency={emergency:.0f}", end='\r')

        # Early exit if garbage ratio drops below threshold
        if gc_ratio < 0.05 and i > 10:
            print(f"\n  Garbage ratio reached {gc_ratio:.1%} — settling complete early.")
            break

    print()
    observer.stop()
    observer.join(timeout=5)

    result = print_compaction_report(phase_results, observer.samples, args.settle_time)

    # Export
    os.makedirs(args.output_dir, exist_ok=True)
    ts = time.strftime('%Y%m%d-%H%M%S')

    json_path = os.path.join(args.output_dir, f'compaction_{ts}.json')
    with open(json_path, 'w') as f:
        json.dump(result, f, indent=2)

    samples_path = os.path.join(args.output_dir, f'compaction_timeseries_{ts}.csv')
    if observer.samples:
        with open(samples_path, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=observer.samples[0].keys())
            w.writeheader()
            w.writerows(observer.samples)
        print(f"GC timeseries written to: {samples_path}")

    print(f"Results written to: {json_path}")
    pool.close()


if __name__ == '__main__':
    main()
