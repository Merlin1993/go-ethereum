package cachetrie

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// Write mode enumeration
type WriteMode int

const (
	ModeNormal    WriteMode = iota // Normal mode
	ModeSpike10x                   // Mode 1: Every 200 writes, intensity increases 10x, lasts 10 times
	ModeSpike2x                    // Mode 2: Every 200 writes, intensity increases 2x, lasts 200 times
	ModeGradual10                  // Mode 3: Every 200 writes increases by 10% (not compound)
)

func TestCachePerformance(t *testing.T) {
	stateCount := 5000
	iterationCount := 4000 // Statistics loop count
	maxSize := 1000000     // Initial storage size limit
	windowMultiple := 256
	// Test normal mode
	t.Run(fmt.Sprintf("NormalMode_StateCount_%d", stateCount), func(t *testing.T) {
		testCacheTrieWithData(t, stateCount, iterationCount, windowMultiple, maxSize, ModeNormal)
	})
	//
	// Test mode 1: Spike 10x mode
	t.Run(fmt.Sprintf("Spike10xMode_StateCount_%d", stateCount), func(t *testing.T) {
		testCacheTrieWithData(t, stateCount, iterationCount, windowMultiple, maxSize, ModeSpike10x)
	})

	// Test mode 2: Spike 2x mode
	t.Run(fmt.Sprintf("Spike2xMode_StateCount_%d", stateCount), func(t *testing.T) {
		testCacheTrieWithData(t, stateCount, iterationCount, windowMultiple, maxSize, ModeSpike2x)
	})

	// Test mode 3: Gradual 10% mode
	t.Run(fmt.Sprintf("Gradual10pMode_StateCount_%d", stateCount), func(t *testing.T) {
		testCacheTrieWithData(t, stateCount, iterationCount, windowMultiple, maxSize, ModeGradual10)
	})
}

// Calculate the write intensity multiplier for current iteration
func calculateWriteIntensity(iteration, baseStateCount int, mode WriteMode) int {
	switch mode {
	case ModeNormal:
		return 100
	case ModeSpike10x:
		// Every 200 writes, intensity increases 10x, lasts 10 times
		positionInCycle := iteration % 350
		if positionInCycle < 50 {
			return 1000
		}
		return 100
	case ModeSpike2x:
		// Every 200 writes, intensity increases 2x, lasts 200 times
		positionInCycle := iteration % 200
		if positionInCycle < 200 {
			return 200
		}
		return 100
	case ModeGradual10:
		// Every 200 writes increases by 10% (not compound)
		cycles := iteration / 400
		return 100 + cycles*20
	default:
		return 100
	}
}

// testCacheTrieWithStateCount performs CacheTrie test with specified state count
func testCacheTrieWithData(t *testing.T, stateCount, iterationCount, windowMultiple, maxSize int, mod WriteMode) {
	t.Logf("Starting test: Single write state count=%d, Statistics loop count=%d, Initial storage size=%d", stateCount, iterationCount, maxSize)

	// Create CacheTrie instance
	cacheTrie := NewCacheTrie(startBlockNum, uint64(windowMultiple), maxSize)

	// Warmup phase - execute until first cleanup
	t.Log("Starting warmup phase...")
	preWarmupStartTime := time.Now()

	// Set initial block height
	currentBlock := uint64(startBlockNum)
	cacheTrie.SetBlockNum(currentBlock)

	// Record initial state
	initialHRW := cacheTrie.GetHRW()
	initialThreshold := initialHRW.GetThreshold()
	t.Logf("Initial state: Threshold(ssthresh)=%d", initialThreshold)

	// Perform warmup until first cleanup occurs
	warmupBatchSize := 10000 // Write count per batch
	warmupBatches := 0

	for i := 0; i < warmupStateCount; i += warmupBatchSize {
		batchSize := warmupBatchSize
		if i+warmupBatchSize > warmupStateCount {
			batchSize = warmupStateCount - i
		}

		// Write data
		for j := 0; j < batchSize; j++ {
			key, value := generateRandomData()
			cacheTrie.Update(key, value, true)
		}

		// Get hash, this triggers cleanup mechanism
		hash, _, kvList := cacheTrie.Hash()

		if kvList != nil && len(kvList.Data) > 0 {
			go func() {
				cacheTrie.FinishCleanup(currentBlock, hash)
			}()
		}

		currentBlock++
		cacheTrie.SetBlockNum(currentBlock)

		warmupBatches++

		// Check if cleanup occurred
		currentCleanupCount := cacheTrie.GetCleanupCount()
		if currentCleanupCount > 0 {
			t.Logf("Warmup phase detected cleanup occurred, batch count=%d, written state count=%d", warmupBatches, (warmupBatches-1)*warmupBatchSize+batchSize)
			break
		}

		// If too much data written without triggering cleanup, end warmup early
		if i+batchSize >= warmupStateCount {
			t.Logf("Warmup phase ended, no cleanup detected, written state count=%d", i+batchSize)
		}
	}
	CleanupTime = 0

	preWarmupDuration := time.Since(preWarmupStartTime)
	t.Logf("Warmup phase completed, duration: %v", preWarmupDuration)

	// Formal test phase
	t.Log("Starting formal test phase...")

	// Record statistics for each operation
	type IterationStats struct {
		WriteTime       time.Duration // Write duration
		HashTime        time.Duration // Hash calculation duration
		WriteSpeed      float64       // Write speed (states/second)
		Size            int           // Current size
		Threshold       int           // Current threshold
		CleanupOccurred bool          // Whether cleanup occurred
		CleanupTime     time.Duration // Cleanup duration (if occurred)
		CleanSize       int
	}

	stats := make([]IterationStats, iterationCount)

	// Prepare CSV data
	csvRecords := [][]string{
		{"Iteration", "Write Time(ns)", "Hash Time(ns)", "Write Speed(states/sec)", "Size", "Threshold", "Cleanup", "Cleanup Time(ns)", "Clean Count"},
	}

	// Reset cleanup time statistics
	cacheTrie.ResetCleanupTimes()

	for i := 0; i < iterationCount; i++ {
		// Record write start time
		writeStart := time.Now()
		newStateCount := stateCount * calculateWriteIntensity(i, stateCount, mod) / 100

		// Write specified number of states
		for j := 0; j < newStateCount; j++ {
			// 50% probability use normal key-value, 50% probability use key-value with address
			if rand.Intn(2) == 0 {
				key, value := generateRandomData()
				cacheTrie.Update(key, value, true)
			} else {
				addr := generateRandomAddress()
				key, value := generateRandomData()
				cacheTrie.UpdateWithAddress(addr, key, value, true)
			}
		}

		writeTime := time.Since(writeStart)

		// Record current cleanup count
		beforeHashCleanupCount := cacheTrie.GetCleanupCount()
		beforeCleanupTotalTime, _ := cacheTrie.GetCleanupTimes()

		// Get hash, this triggers cleanup mechanism
		hashStart := time.Now()
		hash, _, kvList := cacheTrie.Hash()
		// This operation doesn't affect hash value, so it can be done later
		hashTime := time.Since(hashStart) - CleanupTime
		CleanupTime = 0

		if kvList != nil {
			go func() {
				cacheTrie.FinishCleanup(currentBlock, hash)
			}()
		}

		// Move to next block
		currentBlock++
		cacheTrie.SetBlockNum(currentBlock)

		// Check if cleanup occurred
		afterHashCleanupCount := cacheTrie.GetCleanupCount()
		cleanupOccurred := afterHashCleanupCount > beforeHashCleanupCount

		// Calculate cleanup duration (if occurred)
		var cleanupTime time.Duration
		if cleanupOccurred {
			afterCleanupTotalTime, _ := cacheTrie.GetCleanupTimes()
			cleanupTime = afterCleanupTotalTime - beforeCleanupTotalTime
		}

		// Calculate write speed (states/second)
		writeSpeed := float64(stateCount) / writeTime.Seconds()

		// Get current size and threshold
		currentSize := cacheTrie.GetSize()
		currentThreshold := cacheTrie.GetHRW().GetAllSize()

		cleanSize := 0
		if kvList != nil && len(kvList.Data) > 0 {
			cleanSize = len(kvList.Data)
		}
		// Save statistics
		stats[i] = IterationStats{
			WriteTime:       writeTime,
			HashTime:        hashTime,
			WriteSpeed:      writeSpeed,
			Size:            currentSize,
			Threshold:       currentThreshold,
			CleanupOccurred: cleanupOccurred,
			CleanupTime:     cleanupTime,
			CleanSize:       cleanSize,
		}

		// Add to CSV records
		csvRecords = append(csvRecords, []string{
			strconv.Itoa(i + 1),
			strconv.FormatInt(writeTime.Nanoseconds(), 10),
			strconv.FormatInt(hashTime.Nanoseconds(), 10),
			strconv.FormatFloat(writeSpeed, 'f', 2, 64),
			strconv.Itoa(currentSize),
			strconv.Itoa(currentThreshold),
			strconv.FormatBool(cleanupOccurred),
			strconv.FormatInt(cleanupTime.Nanoseconds(), 10),
		})

		// Output current iteration statistics
		cleanupStatus := "None"
		if cleanupOccurred {
			cleanupStatus = fmt.Sprintf("Occurred, duration: %v", cleanupTime)
		}

		t.Logf("Iteration %d/%d: Write time=%v, Speed=%.2f states/sec, Hash time=%v, Size=%d, Threshold=%d, Cleanup: %s",
			i+1, iterationCount, writeTime, writeSpeed, hashTime.String(), currentSize, currentThreshold, cleanupStatus)
	}

	// Calculate average statistics
	var totalWriteTime, totalHashTime, totalCleanupTime time.Duration
	var totalWriteSpeed float64
	cleanupCount := 0

	for _, stat := range stats {
		totalWriteTime += stat.WriteTime
		totalHashTime += stat.HashTime
		totalWriteSpeed += stat.WriteSpeed

		if stat.CleanupOccurred {
			cleanupCount++
			totalCleanupTime += stat.CleanupTime
		}
	}

	avgWriteTime := totalWriteTime / time.Duration(iterationCount)
	avgHashTime := totalHashTime / time.Duration(iterationCount)
	avgWriteSpeed := totalWriteSpeed / float64(iterationCount)

	var avgCleanupTime time.Duration
	if cleanupCount > 0 {
		avgCleanupTime = totalCleanupTime / time.Duration(cleanupCount)
	}

	// Output summary statistics
	t.Logf("\n===== Test Summary (State Count: %d) =====", stateCount)
	t.Logf("Average write time: %v", avgWriteTime)
	t.Logf("Average write speed: %.2f states/sec", avgWriteSpeed)
	t.Logf("Average hash time: %v", avgHashTime)
	t.Logf("Cleanup trigger count: %d/%d", cleanupCount, iterationCount)

	if cleanupCount > 0 {
		t.Logf("Average cleanup time: %v", avgCleanupTime)
	}

	// Get final hit/miss statistics
	totalGetRequests, hitCount, missCount, getHitRate,
		totalUpdateRequests, updateHitCount, updateMissCount, updateHitRate := cacheTrie.GetHitRate()

	t.Logf("Get operations: Total requests=%d, Hits=%d, Misses=%d, Hit rate=%.2f%%",
		totalGetRequests, hitCount, missCount, getHitRate*100)
	t.Logf("Update operations: Total requests=%d, Hits=%d, Misses=%d, Hit rate=%.2f%%",
		totalUpdateRequests, updateHitCount, updateMissCount, updateHitRate*100)

	// Get memory usage information
	memSize := cacheTrie.GetMemorySize()
	t.Logf("Memory usage: %d bytes (%.2f MB)", memSize, float64(memSize)/(1024*1024))

	// Write results to CSV file
	csvFileName := fmt.Sprintf("cacheTrie_states%d_iter%d_window%d_maxsize%d_mod%d.csv",
		stateCount, iterationCount, windowMultiple, maxSize, mod)
	writeCSVFile(t, csvFileName, csvRecords)

	// Add summary statistics to summary CSV
	writeCSVSummary(t, stateCount, iterationCount, windowMultiple, maxSize,
		avgWriteTime, avgHashTime, avgWriteSpeed, cleanupCount,
		avgCleanupTime, getHitRate, updateHitRate, uint64(memSize))
}
