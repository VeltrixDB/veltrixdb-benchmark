// dbbench — db_bench-style benchmark tool for VeltrixDB.
//
// Mirrors the interface of RocksDB's db_bench so that VeltrixDB performance
// numbers are directly comparable to published RocksDB benchmarks.
//
// Usage:
//
//	go run . --benchmarks=fillrandom,readrandom --num=1000000 --threads=16
//
// Available benchmarks (run comma-separated in --benchmarks):
//
//	fillseq          — sequential PUT (key000000000000, key000000000001, …)
//	fillrandom        — random PUT across [0, num) key space
//	fillbatch         — random batch PUT (--batch_size entries per MPUT)
//	overwrite         — like fillrandom but keys already exist (tests overwrite path)
//	readseq           — sequential GET
//	readrandom        — random GET across [0, num) key space
//	readmissing       — random GET for keys that do not exist (tests NOT_FOUND path)
//	readwhilewriting  — concurrent: N read threads + 1 background write thread
//	deleteseq         — sequential DEL
//	deleterandom      — random DEL
//	stats             — print VeltrixDB INFO and exit
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ── CLI flags ─────────────────────────────────────────────────────────────────

var (
	flagHost          = flag.String("host", "127.0.0.1", "VeltrixDB hostname")
	flagPort          = flag.Int("port", 9000, "VeltrixDB port")
	flagBenchmarks    = flag.String("benchmarks", "fillrandom,readrandom",
		"Comma-separated list of benchmarks to run")
	flagNum       = flag.Int64("num", 1_000_000, "Number of key-value pairs")
	flagValueSize = flag.Int("value_size", 1024, "Value size in bytes")
	flagKeySize   = flag.Int("key_size", 16,
		"Key prefix size in bytes (minimum 8; the suffix is always a zero-padded int64)")
	flagThreads   = flag.Int("threads", 1, "Number of concurrent goroutines")
	flagBatchSize = flag.Int("batch_size", 100,
		"Keys per MPUT/MGET call (used by fillbatch / readbatch)")
	flagHistogram     = flag.Bool("histogram", false, "Print per-operation latency histogram")
	flagStatsInterval = flag.Int("stats_interval", 0,
		"Print running throughput every N seconds (0 = disabled)")
	flagDuration = flag.Int("duration", 0,
		"Run each benchmark for N seconds instead of --num operations (0 = use --num)")
	flagUsername   = flag.String("username", "", "AUTH username (leave empty if no auth)")
	flagPassword   = flag.String("password", "", "AUTH password")
	flagReportFile = flag.String("report_file", "", "Write JSON result summary to this file")
)

// ── Key / value generation ────────────────────────────────────────────────────

// formatKey builds a key string of the form "<prefix><zero-padded-n>".
// The prefix length is padded or trimmed to keySize-8 bytes; the suffix is
// always 8 zero-padded decimal digits, giving a reproducible lexicographic
// ordering identical to db_bench.
func formatKey(prefix string, n int64) []byte {
	return []byte(fmt.Sprintf("%s%016d", prefix, n))
}

// randomKey picks a random key index in [0, keyCount).
func randomKey(rng *rand.Rand, keyCount int64) int64 {
	return rng.Int63n(keyCount)
}

// makeValue returns a deterministic value of the given size seeded by n.
// Using a fixed pattern avoids allocating random bytes on the hot path.
func makeValue(size int, n int64) []byte {
	v := make([]byte, size)
	var seed [8]byte
	binary.LittleEndian.PutUint64(seed[:], uint64(n))
	for i := 0; i < size; i++ {
		v[i] = seed[i%8] ^ byte(i)
	}
	return v
}

// ── Workload runner ───────────────────────────────────────────────────────────

// counter is a shared atomic operation counter used to distribute work across
// goroutines without locks (same model as db_bench's SharedState).
type counter struct{ n int64 }

func (c *counter) next() (int64, bool) {
	v := atomic.AddInt64(&c.n, 1) - 1
	return v, true
}

// runParallel fans the workload fn out to cfg.threads goroutines.
// Each goroutine receives a *Stats and a *rand.Rand seeded differently.
// Returns merged Stats over all goroutines.
func runParallel(threads int, fn func(id int, rng *rand.Rand, s *Stats)) *Stats {
	perThread := make([]*Stats, threads)
	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		perThread[i] = newStats()
		id := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*1_000_003))
			fn(id, rng, perThread[id])
		}()
	}
	wg.Wait()

	merged := newStats()
	for _, s := range perThread {
		merged.merge(s)
	}
	return merged
}

// ── Benchmark definitions ─────────────────────────────────────────────────────

// cfg holds the resolved flags for convenience.
type cfg struct {
	host      string
	port      int
	num       int64
	valSize   int
	keySize   int
	threads   int
	batchSize int
	histogram bool
	duration  time.Duration
	username  string
	password  string
}

const keyPrefix = "key"

func runFillSeq(pool *Pool, c cfg) *Stats {
	ctr := &counter{}
	start := time.Now()
	stats := runParallel(c.threads, func(id int, rng *rand.Rand, s *Stats) {
		value := makeValue(c.valSize, int64(id))
		for {
			i, _ := ctr.next()
			if i >= c.num || (c.duration > 0 && time.Since(start) >= c.duration) {
				return
			}
			key := formatKey(keyPrefix, i)
			t0 := time.Now()
			conn := pool.acquire()
			err := conn.put(key, value)
			pool.release(conn)
			if err != nil {
				s.recordError()
				continue
			}
			s.recordOp(time.Since(t0), c.valSize)
		}
	})
	return stats
}

func runFillRandom(pool *Pool, c cfg) *Stats {
	ctr := &counter{}
	start := time.Now()
	return runParallel(c.threads, func(id int, rng *rand.Rand, s *Stats) {
		value := makeValue(c.valSize, int64(id))
		for {
			i, _ := ctr.next()
			if i >= c.num || (c.duration > 0 && time.Since(start) >= c.duration) {
				return
			}
			key := formatKey(keyPrefix, randomKey(rng, c.num))
			t0 := time.Now()
			conn := pool.acquire()
			err := conn.put(key, value)
			pool.release(conn)
			if err != nil {
				s.recordError()
				continue
			}
			s.recordOp(time.Since(t0), c.valSize)
		}
	})
}

func runFillBatch(pool *Pool, c cfg) *Stats {
	totalBatches := (c.num + int64(c.batchSize) - 1) / int64(c.batchSize)
	ctr := &counter{}
	start := time.Now()
	return runParallel(c.threads, func(id int, rng *rand.Rand, s *Stats) {
		keys := make([][]byte, c.batchSize)
		vals := make([][]byte, c.batchSize)
		value := makeValue(c.valSize, int64(id))
		for j := range vals {
			vals[j] = value
		}
		for {
			batch, _ := ctr.next()
			if batch >= totalBatches || (c.duration > 0 && time.Since(start) >= c.duration) {
				return
			}
			for j := 0; j < c.batchSize; j++ {
				keys[j] = formatKey(keyPrefix, randomKey(rng, c.num))
			}
			t0 := time.Now()
			conn := pool.acquire()
			ok, err := conn.multiPut(keys, vals)
			pool.release(conn)
			if err != nil {
				s.recordError()
				continue
			}
			lat := time.Since(t0)
			perOp := lat / time.Duration(c.batchSize)
			for j := 0; j < ok; j++ {
				s.recordOp(perOp, c.valSize)
			}
		}
	})
}

func runReadSeq(pool *Pool, c cfg) *Stats {
	ctr := &counter{}
	start := time.Now()
	return runParallel(c.threads, func(id int, rng *rand.Rand, s *Stats) {
		for {
			i, _ := ctr.next()
			if i >= c.num || (c.duration > 0 && time.Since(start) >= c.duration) {
				return
			}
			key := formatKey(keyPrefix, i)
			t0 := time.Now()
			conn := pool.acquire()
			val, found, err := conn.get(key)
			pool.release(conn)
			if err != nil {
				s.recordError()
				continue
			}
			s.recordOp(time.Since(t0), len(val))
			if found {
				s.recordFound()
			} else {
				s.recordNotFound()
			}
		}
	})
}

func runReadRandom(pool *Pool, c cfg) *Stats {
	ctr := &counter{}
	start := time.Now()
	return runParallel(c.threads, func(id int, rng *rand.Rand, s *Stats) {
		for {
			i, _ := ctr.next()
			if i >= c.num || (c.duration > 0 && time.Since(start) >= c.duration) {
				return
			}
			key := formatKey(keyPrefix, randomKey(rng, c.num))
			t0 := time.Now()
			conn := pool.acquire()
			val, found, err := conn.get(key)
			pool.release(conn)
			if err != nil {
				s.recordError()
				continue
			}
			s.recordOp(time.Since(t0), len(val))
			if found {
				s.recordFound()
			} else {
				s.recordNotFound()
			}
		}
	})
}

// readMissing reads keys guaranteed not to exist (uses key space [num, 2*num)).
func runReadMissing(pool *Pool, c cfg) *Stats {
	ctr := &counter{}
	start := time.Now()
	return runParallel(c.threads, func(id int, rng *rand.Rand, s *Stats) {
		for {
			i, _ := ctr.next()
			if i >= c.num || (c.duration > 0 && time.Since(start) >= c.duration) {
				return
			}
			// Shift key space past all loaded keys so they definitely don't exist.
			key := formatKey(keyPrefix, c.num+randomKey(rng, c.num))
			t0 := time.Now()
			conn := pool.acquire()
			_, found, err := conn.get(key)
			pool.release(conn)
			if err != nil {
				s.recordError()
				continue
			}
			s.recordOp(time.Since(t0), 0)
			if found {
				s.recordFound()
			} else {
				s.recordNotFound()
			}
		}
	})
}

// readWhileWriting runs (threads-1) reader goroutines and 1 background writer.
// The writer is a dedicated goroutine not counted in the reported read stats.
func runReadWhileWriting(pool *Pool, c cfg) *Stats {
	stop := make(chan struct{})
	start := time.Now()

	// Background writer
	go func() {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		value := makeValue(c.valSize, 0)
		for {
			select {
			case <-stop:
				return
			default:
			}
			key := formatKey(keyPrefix, randomKey(rng, c.num))
			conn := pool.acquire()
			conn.put(key, value) //nolint:errcheck
			pool.release(conn)
		}
	}()

	readers := c.threads - 1
	if readers < 1 {
		readers = 1
	}
	ctr := &counter{}
	stats := runParallel(readers, func(id int, rng *rand.Rand, s *Stats) {
		for {
			i, _ := ctr.next()
			if i >= c.num || (c.duration > 0 && time.Since(start) >= c.duration) {
				return
			}
			key := formatKey(keyPrefix, randomKey(rng, c.num))
			t0 := time.Now()
			conn := pool.acquire()
			val, found, err := conn.get(key)
			pool.release(conn)
			if err != nil {
				s.recordError()
				continue
			}
			s.recordOp(time.Since(t0), len(val))
			if found {
				s.recordFound()
			} else {
				s.recordNotFound()
			}
		}
	})
	close(stop)
	return stats
}

func runOverwrite(pool *Pool, c cfg) *Stats {
	// Identical to fillRandom — on an already-loaded dataset these are overwrites.
	return runFillRandom(pool, c)
}

func runDeleteSeq(pool *Pool, c cfg) *Stats {
	ctr := &counter{}
	start := time.Now()
	return runParallel(c.threads, func(id int, rng *rand.Rand, s *Stats) {
		for {
			i, _ := ctr.next()
			if i >= c.num || (c.duration > 0 && time.Since(start) >= c.duration) {
				return
			}
			key := formatKey(keyPrefix, i)
			t0 := time.Now()
			conn := pool.acquire()
			err := conn.del(key)
			pool.release(conn)
			if err != nil {
				s.recordError()
				continue
			}
			s.recordOp(time.Since(t0), 0)
		}
	})
}

func runDeleteRandom(pool *Pool, c cfg) *Stats {
	ctr := &counter{}
	start := time.Now()
	return runParallel(c.threads, func(id int, rng *rand.Rand, s *Stats) {
		for {
			i, _ := ctr.next()
			if i >= c.num || (c.duration > 0 && time.Since(start) >= c.duration) {
				return
			}
			key := formatKey(keyPrefix, randomKey(rng, c.num))
			t0 := time.Now()
			conn := pool.acquire()
			err := conn.del(key)
			pool.release(conn)
			if err != nil {
				s.recordError()
				continue
			}
			s.recordOp(time.Since(t0), 0)
		}
	})
}

func runStats(pool *Pool, c cfg) *Stats {
	conn := pool.acquire()
	info, err := conn.info()
	pool.release(conn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "INFO error: %v\n", err)
		return newStats()
	}
	fmt.Println(info)
	return newStats()
}

// ── Main ──────────────────────────────────────────────────────────────────────

type result struct {
	name    string
	elapsed time.Duration
	stats   *Stats
}

func main() {
	flag.Parse()

	c := cfg{
		host:      *flagHost,
		port:      *flagPort,
		num:       *flagNum,
		valSize:   *flagValueSize,
		keySize:   *flagKeySize,
		threads:   *flagThreads,
		batchSize: *flagBatchSize,
		histogram: *flagHistogram,
		username:  *flagUsername,
		password:  *flagPassword,
	}
	if *flagDuration > 0 {
		c.duration = time.Duration(*flagDuration) * time.Second
		// In duration mode, num is set large so the counter never wins.
		c.num = 1<<62 - 1
	}

	// Dial a connection pool with one connection per worker goroutine.
	pool, err := newPool(c.host, c.port, c.threads+2, 10*time.Second, c.username, c.password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.close()

	// Quick connectivity check
	probe := pool.acquire()
	if err := probe.ping(); err != nil {
		fmt.Fprintf(os.Stderr, "ping failed: %v\n", err)
		os.Exit(1)
	}
	pool.release(probe)

	printHeader(c)

	benchmarks := strings.Split(*flagBenchmarks, ",")
	var results []result

	for _, name := range benchmarks {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		// Optional periodic progress reporter
		var stopProgress chan struct{}
		if *flagStatsInterval > 0 {
			stopProgress = startProgressReporter(name, *flagStatsInterval)
		}

		t0 := time.Now()
		var stats *Stats

		switch name {
		case "fillseq":
			stats = runFillSeq(pool, c)
		case "fillrandom":
			stats = runFillRandom(pool, c)
		case "fillbatch":
			stats = runFillBatch(pool, c)
		case "overwrite":
			stats = runOverwrite(pool, c)
		case "readseq":
			stats = runReadSeq(pool, c)
		case "readrandom":
			stats = runReadRandom(pool, c)
		case "readmissing":
			stats = runReadMissing(pool, c)
		case "readwhilewriting":
			stats = runReadWhileWriting(pool, c)
		case "deleteseq":
			stats = runDeleteSeq(pool, c)
		case "deleterandom":
			stats = runDeleteRandom(pool, c)
		case "stats":
			stats = runStats(pool, c)
		default:
			fmt.Fprintf(os.Stderr, "unknown benchmark: %q\n", name)
			fmt.Fprintln(os.Stderr, "available: fillseq, fillrandom, fillbatch, overwrite,")
			fmt.Fprintln(os.Stderr, "           readseq, readrandom, readmissing, readwhilewriting,")
			fmt.Fprintln(os.Stderr, "           deleteseq, deleterandom, stats")
			continue
		}

		elapsed := time.Since(t0)
		if stopProgress != nil {
			close(stopProgress)
		}

		stats.report(name, elapsed, c.valSize, c.histogram)
		results = append(results, result{name: name, elapsed: elapsed, stats: stats})
	}

	if *flagReportFile != "" {
		writeJSONReport(*flagReportFile, results, c)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func printHeader(c cfg) {
	fmt.Printf("VeltrixDB dbbench\n")
	fmt.Printf("  host       : %s:%d\n", c.host, c.port)
	fmt.Printf("  num        : %d\n", c.num)
	fmt.Printf("  value_size : %d B\n", c.valSize)
	fmt.Printf("  threads    : %d\n", c.threads)
	fmt.Printf("  batch_size : %d\n", c.batchSize)
	if c.duration > 0 {
		fmt.Printf("  duration   : %s\n", c.duration)
	}
	fmt.Println()
}

// startProgressReporter prints ops/s to stdout every intervalSec seconds
// until the returned channel is closed.
func startProgressReporter(name string, intervalSec int) chan struct{} {
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-stop:
				return
			case t := <-ticker.C:
				fmt.Printf("  [%s] elapsed %s\n", name, t.Sub(start).Round(time.Second))
			}
		}
	}()
	return stop
}

// writeJSONReport writes a minimal JSON summary that can be ingested by
// dashboards or compared programmatically against RocksDB db_bench output.
func writeJSONReport(path string, results []result, c cfg) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "report: %v\n", err)
		return
	}
	defer f.Close()

	fmt.Fprintln(f, "{")
	fmt.Fprintf(f, `  "config": {`+"\n")
	fmt.Fprintf(f, `    "host": %q,`+"\n", c.host)
	fmt.Fprintf(f, `    "port": %d,`+"\n", c.port)
	fmt.Fprintf(f, `    "num": %d,`+"\n", c.num)
	fmt.Fprintf(f, `    "value_size": %d,`+"\n", c.valSize)
	fmt.Fprintf(f, `    "threads": %d,`+"\n", c.threads)
	fmt.Fprintf(f, `    "batch_size": %d`+"\n", c.batchSize)
	fmt.Fprintln(f, `  },`)
	fmt.Fprintln(f, `  "benchmarks": [`)

	for i, r := range results {
		ops := r.stats.ops
		elapsedSec := r.elapsed.Seconds()
		opsSec := 0.0
		microsPerOp := 0.0
		p50, p99, p999 := 0.0, 0.0, 0.0
		if ops > 0 {
			opsSec = float64(ops) / elapsedSec
			microsPerOp = elapsedSec * 1e6 / float64(ops)
			p50 = r.stats.hist.percentile(50)
			p99 = r.stats.hist.percentile(99)
			p999 = r.stats.hist.percentile(99.9)
		}
		comma := ","
		if i == len(results)-1 {
			comma = ""
		}
		fmt.Fprintf(f, `    {`+"\n")
		fmt.Fprintf(f, `      "name": %q,`+"\n", r.name)
		fmt.Fprintf(f, `      "ops": %d,`+"\n", ops)
		fmt.Fprintf(f, `      "elapsed_s": %.3f,`+"\n", elapsedSec)
		fmt.Fprintf(f, `      "ops_per_sec": %.1f,`+"\n", opsSec)
		fmt.Fprintf(f, `      "micros_per_op": %.3f,`+"\n", microsPerOp)
		fmt.Fprintf(f, `      "errors": %d,`+"\n", r.stats.errors)
		fmt.Fprintf(f, `      "p50_us": %.1f,`+"\n", p50)
		fmt.Fprintf(f, `      "p99_us": %.1f,`+"\n", p99)
		fmt.Fprintf(f, `      "p99_9_us": %.1f`+"\n", p999)
		fmt.Fprintf(f, `    }%s`+"\n", comma)
	}

	fmt.Fprintln(f, `  ]`)
	fmt.Fprintln(f, "}")
	fmt.Printf("Report written to %s\n", path)
}
