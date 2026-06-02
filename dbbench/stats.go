package main

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"
)

// ── Histogram ─────────────────────────────────────────────────────────────────
// Bucket boundaries match db_bench exactly (microseconds).

var bucketBounds = []float64{
	1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
	12, 14, 16, 18, 20, 25, 30, 35, 40, 45,
	50, 60, 70, 80, 90, 100,
	120, 140, 160, 180, 200, 250, 300, 350, 400, 450,
	500, 600, 700, 800, 900, 1000,
	1200, 1400, 1600, 1800, 2000, 2500, 3000, 3500, 4000, 4500,
	5000, 6000, 7000, 8000, 9000, 10000,
	12000, 14000, 16000, 18000, 20000, 25000, 30000, 35000, 40000, 45000,
	50000, 60000, 70000, 80000, 90000, 100000,
	120000, 140000, 160000, 180000, 200000, 250000, 300000, 350000, 400000, 450000,
	500000, 600000, 700000, 800000, 900000, 1000000,
}

type Histogram struct {
	counts  []int64
	min     float64
	max     float64
	sum     float64
	sumSq   float64
	total   int64
	_minSet int32 // 0 = not set yet
}

func newHistogram() *Histogram {
	return &Histogram{
		counts: make([]int64, len(bucketBounds)+1),
		min:    math.MaxFloat64,
	}
}

func (h *Histogram) add(us float64) {
	atomic.AddInt64(&h.total, 1)
	h.sum += us
	h.sumSq += us * us

	if us < h.min {
		h.min = us
	}
	if us > h.max {
		h.max = us
	}

	// Binary search for bucket
	lo, hi := 0, len(bucketBounds)
	for lo < hi {
		mid := (lo + hi) / 2
		if us < bucketBounds[mid] {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	h.counts[lo]++
}

// merge combines another histogram into this one (for aggregating per-thread stats).
func (h *Histogram) merge(other *Histogram) {
	for i, c := range other.counts {
		h.counts[i] += c
	}
	if other.min < h.min {
		h.min = other.min
	}
	if other.max > h.max {
		h.max = other.max
	}
	h.sum += other.sum
	h.sumSq += other.sumSq
	h.total += other.total
}

func (h *Histogram) percentile(p float64) float64 {
	total := h.total
	if total == 0 {
		return 0
	}
	threshold := int64(math.Ceil(float64(total) * p / 100.0))
	cumulative := int64(0)
	for i, c := range h.counts {
		cumulative += c
		if cumulative >= threshold {
			// Return the bucket lower bound
			if i == 0 {
				return 0
			}
			return bucketBounds[i-1]
		}
	}
	return h.max
}

func (h *Histogram) average() float64 {
	if h.total == 0 {
		return 0
	}
	return h.sum / float64(h.total)
}

func (h *Histogram) stddev() float64 {
	if h.total == 0 {
		return 0
	}
	avg := h.average()
	variance := h.sumSq/float64(h.total) - avg*avg
	if variance < 0 {
		return 0
	}
	return math.Sqrt(variance)
}

// print renders the histogram in db_bench format.
func (h *Histogram) print(label string) {
	if h.total == 0 {
		return
	}
	fmt.Printf("Microseconds per %s:\n", label)
	fmt.Printf("Count: %d  Average: %.2f  StdDev: %.2f\n",
		h.total, h.average(), h.stddev())
	minVal := h.min
	if minVal == math.MaxFloat64 {
		minVal = 0
	}
	fmt.Printf("Min: %.0f  Median: %.1f  Max: %.0f\n",
		minVal, h.percentile(50), h.max)
	fmt.Printf("Percentiles: P50: %.1f P75: %.1f P99: %.1f P99.9: %.1f P99.99: %.1f\n",
		h.percentile(50), h.percentile(75),
		h.percentile(99), h.percentile(99.9), h.percentile(99.99))
	fmt.Println(strings.Repeat("-", 54))

	// Print buckets that have counts
	maxCount := int64(0)
	for _, c := range h.counts {
		if c > maxCount {
			maxCount = c
		}
	}
	cumPct := 0.0
	for i, c := range h.counts {
		if c == 0 {
			continue
		}
		lo := 0.0
		hi := 0.0
		if i == 0 {
			hi = bucketBounds[0]
		} else if i < len(bucketBounds) {
			lo = bucketBounds[i-1]
			hi = bucketBounds[i]
		} else {
			lo = bucketBounds[len(bucketBounds)-1]
			hi = math.Inf(1)
		}
		pct := float64(c) / float64(h.total) * 100.0
		cumPct += pct
		barLen := int(float64(c) / float64(maxCount) * 20)
		bar := strings.Repeat("#", barLen)
		if math.IsInf(hi, 1) {
			fmt.Printf("[ %7.0f, inf     ) %9d %6.3f%% %7.3f%% %s\n",
				lo, c, pct, cumPct, bar)
		} else {
			fmt.Printf("[ %7.0f, %7.0f ) %9d %6.3f%% %7.3f%% %s\n",
				lo, hi, c, pct, cumPct, bar)
		}
	}
	fmt.Println()
}

// ── Stats ─────────────────────────────────────────────────────────────────────

// Stats tracks per-workload metrics across all goroutines.
type Stats struct {
	ops      int64 // total operations completed (atomic)
	errors   int64 // total errors (atomic)
	found    int64 // for reads: keys that existed (atomic)
	notFound int64 // for reads: keys that were missing (atomic)
	bytes    int64 // total payload bytes (atomic)

	hist *Histogram
}

func newStats() *Stats {
	return &Stats{hist: newHistogram()}
}

// recordOp records one successful operation with its latency and payload.
func (s *Stats) recordOp(latency time.Duration, payload int) {
	atomic.AddInt64(&s.ops, 1)
	atomic.AddInt64(&s.bytes, int64(payload))
	s.hist.add(float64(latency.Nanoseconds()) / 1000.0) // ns → µs
}

func (s *Stats) recordFound()    { atomic.AddInt64(&s.found, 1) }
func (s *Stats) recordNotFound() { atomic.AddInt64(&s.notFound, 1) }
func (s *Stats) recordError()    { atomic.AddInt64(&s.errors, 1) }

// merge aggregates another Stats into this one.
func (s *Stats) merge(other *Stats) {
	s.ops += other.ops
	s.errors += other.errors
	s.found += other.found
	s.notFound += other.notFound
	s.bytes += other.bytes
	s.hist.merge(other.hist)
}

// report prints the db_bench-style summary line.
//
// Example:
//
//	fillrandom   :       2.441 micros/op;   40.9 MB/s (1000000 ops)
//	readrandom   :       0.912 micros/op; (1000000 ops; 48.3% found)
func (s *Stats) report(name string, elapsed time.Duration, valueSize int, showHistogram bool) {
	ops := s.ops
	if ops == 0 {
		fmt.Printf("%-16s : (no ops)\n", name)
		return
	}
	elapsedSec := elapsed.Seconds()
	microsPerOp := elapsedSec * 1e6 / float64(ops)
	mbPerSec := float64(s.bytes) / (1024 * 1024) / elapsedSec

	readTotal := s.found + s.notFound
	if readTotal > 0 {
		foundPct := float64(s.found) / float64(readTotal) * 100.0
		fmt.Printf("%-16s :  %10.3f micros/op; (%d ops; %.1f%% found)\n",
			name, microsPerOp, ops, foundPct)
	} else {
		fmt.Printf("%-16s :  %10.3f micros/op;  %6.1f MB/s (%d ops)\n",
			name, microsPerOp, mbPerSec, ops)
	}

	if s.errors > 0 {
		fmt.Printf("  Errors: %d\n", s.errors)
	}

	if showHistogram {
		opLabel := name
		if readTotal > 0 {
			opLabel = "read"
		}
		s.hist.print(opLabel)
	}
}
