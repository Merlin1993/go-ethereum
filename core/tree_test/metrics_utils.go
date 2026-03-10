package tree

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(windowSize int, baseDir string) *MetricsCollector {
	return &MetricsCollector{
		windowSize: windowSize,
		baseDir:    baseDir,
	}
}

// AddInjected tracks injected items
func (c *MetricsCollector) AddInjected(n int) {
	c.totalInjected += int64(n)
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

// GetMetricsString returns a formatted metrics string
func (c *MetricsCollector) GetMetricsString() string {
	rss := GetRSS()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	avgRoot, maxRoot := c.getTimingStats()
	dirSize, _ := GetDirSize(c.baseDir)

	return fmt.Sprintf("Storage_Bytes: %s, Injected_Items: %.1fM, Avg_Root: %.2fms, Max_Root: %.2fms, RSS: %s, Heap: %s",
		bytesToReadable(uint64(dirSize)),
		float64(c.totalInjected)/1000000.0,
		avgRoot,
		maxRoot,
		bytesToReadable(rss),
		bytesToReadable(m.HeapAlloc))
}

func (c *MetricsCollector) getTimingStats() (avg float64, max float64) {
	if len(c.rootTimes) == 0 {
		return 0, 0
	}
	var total time.Duration
	var maxTime time.Duration
	for _, t := range c.rootTimes {
		total += t
		if t > maxTime {
			maxTime = t
		}
	}
	avg = float64(total.Nanoseconds()) / float64(len(c.rootTimes)) / 1000000.0
	max = float64(maxTime.Nanoseconds()) / 1000000.0
	return
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
