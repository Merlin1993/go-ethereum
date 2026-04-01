package tree

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"
)

// MetricsCollector tracks various performance metrics
type MetricsCollector struct {
	totalInjected int64
	batchSize     int
	rootTimes     []time.Duration
	baseDir       string
	windowSize    int
	lastReported  int64
	csvFilePath   string
	csvFile       *os.File
	csvWriter     *csv.Writer

	// Sliding window for keys (LRU-like via FIFO)
	keyPool   [][]byte
	poolIndex int
	maxPool   int
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(windowSize int, baseDir string, csvName string) *MetricsCollector {
	c := &MetricsCollector{
		windowSize: windowSize,
		baseDir:    baseDir,
		maxPool:    10000000, // 10M keys as requested
		keyPool:    make([][]byte, 0, 10000000),
	}

	if csvName != "" {
		resultsDir := "results"
		os.MkdirAll(resultsDir, 0755)
		c.csvFilePath = filepath.Join(resultsDir, csvName)
		f, err := os.Create(c.csvFilePath)
		if err == nil {
			c.csvFile = f
			c.csvWriter = csv.NewWriter(f)
			header := []string{
				"Total_Injected", "Avg_Root_ms", "Max_Root_ms",
				"P95_ms", "P99_ms", "Min_ms", "Q1_ms", "Median_ms", "Q3_ms",
				"Disk_MB", "RSS_MB", "Heap_MB",
			}
			c.csvWriter.Write(header)
			c.csvWriter.Flush()
		}
	}
	return c
}

// AddInjected tracks injected items
func (c *MetricsCollector) AddInjected(n int, keys [][]byte) {
	c.totalInjected += int64(n)
	// Add keys to pool and maintain size (FIFO logic for LRU-like behavior)
	for _, k := range keys {
		if len(c.keyPool) < c.maxPool {
			c.keyPool = append(c.keyPool, k)
		} else {
			c.keyPool[c.poolIndex] = k
			c.poolIndex = (c.poolIndex + 1) % c.maxPool
		}
	}
}

// AddUpdated tracks updated items and refreshes their "recentness" in our FIFO pool
func (c *MetricsCollector) AddUpdated(keys [][]byte) {
	// For simplicity in a sliding window, we just treat updates as new "recent" entries
	// In a true LRU we'd move them to the end. Here we just re-insert them.
	for _, k := range keys {
		if len(c.keyPool) < c.maxPool {
			c.keyPool = append(c.keyPool, k)
		} else {
			c.keyPool[c.poolIndex] = k
			c.poolIndex = (c.poolIndex + 1) % c.maxPool
		}
	}
}

// GetRandomKeys returns n random keys from the pool
func (c *MetricsCollector) GetRandomKeys(n int) [][]byte {
	if len(c.keyPool) == 0 {
		return nil
	}
	if n > len(c.keyPool) {
		n = len(c.keyPool)
	}
	res := make([][]byte, n)
	for i := 0; i < n; i++ {
		res[i] = c.keyPool[rand.Intn(len(c.keyPool))]
	}
	return res
}

// AddRootTime tracks root calculation time
func (c *MetricsCollector) AddRootTime(d time.Duration) {
	c.rootTimes = append(c.rootTimes, d)
}

// ShouldReport checks if it's time to report metrics
func (c *MetricsCollector) ShouldReport() bool {
	return c.totalInjected >= c.lastReported+int64(c.windowSize)
}

// ResetWindow resets the timing window
func (c *MetricsCollector) ResetWindow() {
	c.rootTimes = nil
	c.lastReported = c.totalInjected
}

// Stats represents advanced timing statistics
type Stats struct {
	Avg, Max, Min  float64
	P95, P99       float64
	Q1, Median, Q3 float64
}

// GetMetricsString returns a formatted metrics string and logs to CSV
func (c *MetricsCollector) GetMetricsString() string {
	rss := GetRSS()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	stats := c.calculateStats()
	dirSize, _ := GetDirSize(c.baseDir)

	// Log to CSV
	if c.csvWriter != nil {
		c.csvWriter.Write([]string{
			fmt.Sprintf("%d", c.totalInjected),
			fmt.Sprintf("%.2f", stats.Avg),
			fmt.Sprintf("%.2f", stats.Max),
			fmt.Sprintf("%.2f", stats.P95),
			fmt.Sprintf("%.2f", stats.P99),
			fmt.Sprintf("%.2f", stats.Min),
			fmt.Sprintf("%.2f", stats.Q1),
			fmt.Sprintf("%.2f", stats.Median),
			fmt.Sprintf("%.2f", stats.Q3),
			fmt.Sprintf("%d", dirSize/(1024*1024)),
			fmt.Sprintf("%d", rss/(1024*1024)),
			fmt.Sprintf("%d", m.HeapAlloc/(1024*1024)),
		})
		c.csvWriter.Flush()
	}

	return fmt.Sprintf("Storage: %s, Total: %.2fM, Pool: %d, Avg: %.2fms, P95: %.2fms, P99: %.2fms, Box[Min: %.1f, Q1: %.1f, Med: %.1f, Q3: %.1f, Max: %.1f], RSS: %s, Heap: %s",
		bytesToReadable(uint64(dirSize)),
		float64(c.totalInjected)/1000000.0,
		len(c.keyPool),
		stats.Avg,
		stats.P95,
		stats.P99,
		stats.Min,
		stats.Q1,
		stats.Median,
		stats.Q3,
		stats.Max,
		bytesToReadable(rss),
		bytesToReadable(m.HeapAlloc))
}

func (c *MetricsCollector) calculateStats() Stats {
	if len(c.rootTimes) == 0 {
		return Stats{}
	}

	times := make([]float64, len(c.rootTimes))
	var total float64
	for i, t := range c.rootTimes {
		val := float64(t.Nanoseconds()) / 1000000.0
		times[i] = val
		total += val
	}
	sort.Float64s(times)

	n := len(times)
	getPercentile := func(p float64) float64 {
		idx := int(p * float64(n-1))
		return times[idx]
	}

	return Stats{
		Avg:    total / float64(n),
		Max:    times[n-1],
		Min:    times[0],
		P95:    getPercentile(0.95),
		P99:    getPercentile(0.99),
		Q1:     getPercentile(0.25),
		Median: getPercentile(0.50),
		Q3:     getPercentile(0.75),
	}
}

// Close closes the CSV file
func (c *MetricsCollector) Close() {
	if c.csvFile != nil {
		c.csvFile.Close()
	}
}

// GetDirSize calculates directory size
func GetDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// formatSize formats numbers into readable text
func formatSize(size int) string {
	if size < 1000 {
		return fmt.Sprintf("%d", size)
	} else if size < 1000000 {
		return fmt.Sprintf("%.1fk", float64(size)/1000)
	} else {
		return fmt.Sprintf("%.1fw", float64(size)/10000)
	}
}

// bytesToReadable converts byte count to readable format
func bytesToReadable(bytes uint64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	if bytes < KB {
		return fmt.Sprintf("%d B", bytes)
	} else if bytes < MB {
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	} else if bytes < GB {
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	} else {
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	}
}

// writeCSVFile writes test results to CSV file
func writeCSVFile(t *testing.T, fileName string, records [][]string) {
	// Ensure results directory exists
	resultsDir := "results"
	if _, err := os.Stat(resultsDir); os.IsNotExist(err) {
		if err := os.Mkdir(resultsDir, 0755); err != nil {
			t.Logf("failed to create results directory: %v", err)
			return
		}
	}

	// Create CSV file
	filePath := filepath.Join(resultsDir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		t.Logf("failed to create CSV file: %v", err)
		return
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write data
	if err := writer.WriteAll(records); err != nil {
		t.Logf("failed to write CSV data: %v", err)
		return
	}

	t.Logf("test results written to CSV: %s", filePath)
}
