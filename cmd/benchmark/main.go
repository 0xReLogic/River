package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// Command line flags
	serverAddr     = flag.String("server", "http://localhost:8080", "Server address")
	numInserts     = flag.Int("inserts", 1000000, "Number of inserts to perform")
	numQueries     = flag.Int("queries", 1000, "Number of queries to perform")
	numThreads     = flag.Int("threads", 4, "Number of threads")
	valueSize      = flag.Int("value-size", 100, "Size of values in bytes")
	reportInterval = flag.Int("report-interval", 1000, "Report progress every N operations")
)

// Statistics
type Stats struct {
	operations     int64
	totalLatencyNs int64
	minLatencyNs   int64
	maxLatencyNs   int64
	p95LatencyNs   int64
	p99LatencyNs   int64
	errorCount     int64
	startTime      time.Time
	latencies      []time.Duration
	latenciesMutex sync.Mutex
}

func newStats() *Stats {
	return &Stats{
		minLatencyNs: int64(^uint64(0) >> 1), // Max int64
		startTime:    time.Now(),
		latencies:    make([]time.Duration, 0, 1000),
	}
}

func (s *Stats) recordLatency(d time.Duration) {
	atomic.AddInt64(&s.operations, 1)
	atomic.AddInt64(&s.totalLatencyNs, int64(d))

	// Update min/max latency
	for {
		min := atomic.LoadInt64(&s.minLatencyNs)
		if int64(d) >= min || atomic.CompareAndSwapInt64(&s.minLatencyNs, min, int64(d)) {
			break
		}
	}

	for {
		max := atomic.LoadInt64(&s.maxLatencyNs)
		if int64(d) <= max || atomic.CompareAndSwapInt64(&s.maxLatencyNs, max, int64(d)) {
			break
		}
	}

	// Record latency for percentile calculation
	s.latenciesMutex.Lock()
	s.latencies = append(s.latencies, d)
	s.latenciesMutex.Unlock()
}

func (s *Stats) recordError() {
	atomic.AddInt64(&s.errorCount, 1)
}

func (s *Stats) calculatePercentiles() {
	s.latenciesMutex.Lock()
	defer s.latenciesMutex.Unlock()

	if len(s.latencies) == 0 {
		return
	}

	// Sort latencies
	sort.Slice(s.latencies, func(i, j int) bool {
		return s.latencies[i] < s.latencies[j]
	})

	// Calculate p95 and p99
	p95Index := int(float64(len(s.latencies)) * 0.95)
	p99Index := int(float64(len(s.latencies)) * 0.99)

	if p95Index < len(s.latencies) {
		s.p95LatencyNs = int64(s.latencies[p95Index])
	}

	if p99Index < len(s.latencies) {
		s.p99LatencyNs = int64(s.latencies[p99Index])
	}
}

func (s *Stats) printStats(operation string) {
	ops := atomic.LoadInt64(&s.operations)
	if ops == 0 {
		fmt.Printf("%s: No operations performed\n", operation)
		return
	}

	s.calculatePercentiles()

	duration := time.Since(s.startTime)
	throughput := float64(ops) / duration.Seconds()

	avgLatency := time.Duration(atomic.LoadInt64(&s.totalLatencyNs) / ops)
	minLatency := time.Duration(atomic.LoadInt64(&s.minLatencyNs))
	maxLatency := time.Duration(atomic.LoadInt64(&s.maxLatencyNs))
	p95Latency := time.Duration(s.p95LatencyNs)
	p99Latency := time.Duration(s.p99LatencyNs)

	fmt.Printf("\n%s Statistics:\n", operation)
	fmt.Printf("  Operations:    %d\n", ops)
	fmt.Printf("  Runtime:       %v\n", duration.Round(time.Millisecond))
	fmt.Printf("  Throughput:    %.2f ops/sec\n", throughput)
	fmt.Printf("  Avg Latency:   %v\n", avgLatency)
	fmt.Printf("  Min Latency:   %v\n", minLatency)
	fmt.Printf("  Max Latency:   %v\n", maxLatency)
	fmt.Printf("  P95 Latency:   %v\n", p95Latency)
	fmt.Printf("  P99 Latency:   %v\n", p99Latency)
	fmt.Printf("  Error Count:   %d\n", atomic.LoadInt64(&s.errorCount))
}

func main() {
	// Parse command line flags
	flag.Parse()

	// Create HTTP client
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Generate random data
	fmt.Println("Generating random data...")
	keys := make([]string, *numInserts)
	values := make([][]byte, *numInserts)

	for i := 0; i < *numInserts; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
		values[i] = make([]byte, *valueSize)
		rand.Read(values[i])
	}

	// Run insert benchmark
	fmt.Printf("Running insert benchmark with %d threads...\n", *numThreads)
	insertStats := runInsertBenchmark(client, keys, values)
	insertStats.printStats("Insert")

	// Run query benchmark
	fmt.Printf("\nRunning query benchmark with %d threads...\n", *numThreads)
	queryStats := runQueryBenchmark(client, keys)
	queryStats.printStats("Query")
}

func runInsertBenchmark(client *http.Client, keys []string, values [][]byte) *Stats {
	stats := newStats()
	var wg sync.WaitGroup

	// Calculate operations per thread
	opsPerThread := (*numInserts + *numThreads - 1) / *numThreads

	// Start worker threads
	for t := 0; t < *numThreads; t++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			start := threadID * opsPerThread
			end := (threadID + 1) * opsPerThread
			if end > *numInserts {
				end = *numInserts
			}

			for i := start; i < end; i++ {
				// Perform PUT operation
				startTime := time.Now()
				err := putKey(client, keys[i], values[i])
				latency := time.Since(startTime)

				if err != nil {
					stats.recordError()
					log.Printf("Error putting key %s: %v", keys[i], err)
				} else {
					stats.recordLatency(latency)
				}

				// Report progress
				ops := atomic.LoadInt64(&stats.operations)
				if ops%int64(*reportInterval) == 0 {
					elapsed := time.Since(stats.startTime)
					throughput := float64(ops) / elapsed.Seconds()
					fmt.Printf("\rInserts: %d/%d (%.2f ops/sec)", ops, *numInserts, throughput)
				}
			}
		}(t)
	}

	wg.Wait()
	fmt.Println() // New line after progress reports
	return stats
}

func runQueryBenchmark(client *http.Client, keys []string) *Stats {
	stats := newStats()
	var wg sync.WaitGroup

	// Select random keys for querying
	queryKeys := make([]string, *numQueries)
	for i := 0; i < *numQueries; i++ {
		queryKeys[i] = keys[rand.Intn(len(keys))]
	}

	// Calculate operations per thread
	opsPerThread := (*numQueries + *numThreads - 1) / *numThreads

	// Start worker threads
	for t := 0; t < *numThreads; t++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			start := threadID * opsPerThread
			end := (threadID + 1) * opsPerThread
			if end > *numQueries {
				end = *numQueries
			}

			for i := start; i < end; i++ {
				// Perform GET operation
				startTime := time.Now()
				_, err := getKey(client, queryKeys[i])
				latency := time.Since(startTime)

				if err != nil {
					stats.recordError()
					log.Printf("Error getting key %s: %v", queryKeys[i], err)
				} else {
					stats.recordLatency(latency)
				}

				// Report progress
				ops := atomic.LoadInt64(&stats.operations)
				if ops%int64(*reportInterval/10) == 0 {
					elapsed := time.Since(stats.startTime)
					throughput := float64(ops) / elapsed.Seconds()
					fmt.Printf("\rQueries: %d/%d (%.2f ops/sec)", ops, *numQueries, throughput)
				}
			}
		}(t)
	}

	wg.Wait()
	fmt.Println() // New line after progress reports
	return stats
}

func putKey(client *http.Client, key string, value []byte) error {
	url := fmt.Sprintf("%s/put?key=%s", *serverAddr, key)
	req, err := http.NewRequest("POST", url, bytes.NewReader(value))
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func getKey(client *http.Client, key string) ([]byte, error) {
	url := fmt.Sprintf("%s/get?key=%s", *serverAddr, key)
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("key not found")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	value, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return value, nil
}
