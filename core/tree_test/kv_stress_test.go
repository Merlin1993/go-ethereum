package tree

import (
	"flag"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb/leveldb"
)

var (
	kvStressItems            = flag.Int("kvStressItems", method1TotalData, "Total items to inject in TestTrieStressKV")
	kvStressBatchSize        = flag.Int("kvStressBatchSize", method1BatchSize, "Items per write batch in TestTrieStressKV")
	kvStressEpochItems       = flag.Int("kvStressEpochItems", 1000000, "Items per metrics window in TestTrieStressKV")
	kvStressBaseDir          = flag.String("kvStressBaseDir", "F:\\trie_stress_data\\kv", "Base directory for TestTrieStressKV")
	kvStressUpdateRatio      = flag.Int("kvStressUpdateRatio", 100, "Random old-key updates per batch as a percentage of kvStressBatchSize")
	kvStressTargetGB         = flag.Float64("kvStressTargetGB", 0, "Stop TestTrieStressKV after the database directory reaches this many GiB; 0 disables the limit")
	kvStressSizeCheckBatches = flag.Int("kvStressSizeCheckBatches", 1000, "Check kvStressTargetGB every N write batches")
)

// TestTrieStressKV writes the same synthetic workload as TestTrieStressMPT into
// a plain key/value store. It is the no-authentication baseline for storage and
// write cost.
func TestTrieStressKV(t *testing.T) {
	totalData := *kvStressItems
	batchPerCommit := *kvStressBatchSize
	epochItems := *kvStressEpochItems
	baseDir := *kvStressBaseDir
	updateRatio := *kvStressUpdateRatio
	targetBytes := int64(math.Round(*kvStressTargetGB * 1024 * 1024 * 1024))
	sizeCheckBatches := *kvStressSizeCheckBatches
	if totalData <= 0 || batchPerCommit <= 0 || epochItems <= 0 {
		t.Fatalf("kvStressItems, kvStressBatchSize and kvStressEpochItems must all be positive")
	}
	if updateRatio < 0 {
		t.Fatalf("kvStressUpdateRatio must be non-negative")
	}
	if *kvStressTargetGB < 0 || sizeCheckBatches <= 0 {
		t.Fatalf("kvStressTargetGB must be non-negative and kvStressSizeCheckBatches must be positive")
	}

	os.RemoveAll(baseDir)
	if err := os.MkdirAll(baseDir, os.ModePerm); err != nil {
		t.Fatalf("failed to create base directory: %v", err)
	}

	ldb, err := leveldb.New(baseDir, 128, 128, "kv-stress", false)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer ldb.Close()

	collector := NewMetricsCollector(epochItems, baseDir, "kv_stress.csv")
	defer collector.Close()

	totalStart := time.Now()
	batches := 0
	for i := 0; i < totalData; i += batchPerCommit {
		batchSize := batchPerCommit
		if i+batchPerCommit > totalData {
			batchSize = totalData - i
		}

		batch := ldb.NewBatch()
		newKeys := make([][]byte, batchSize)
		for j := 0; j < batchSize; j++ {
			key, value := generateIndexData(i + j)
			if err := batch.Put(key, value); err != nil {
				t.Fatalf("failed to write new key: %v", err)
			}
			newKeys[j] = key
		}

		updateCount := batchSize * updateRatio / 100
		updateKeys := collector.GetRandomKeys(updateCount)
		if len(updateKeys) > 0 {
			for _, key := range updateKeys {
				_, value := generateRandomData()
				if err := batch.Put(key, value); err != nil {
					t.Fatalf("failed to write updated key: %v", err)
				}
			}
			collector.AddUpdated(updateKeys)
		}
		collector.AddInjected(batchSize, newKeys)

		writeStart := time.Now()
		if err := batch.Write(); err != nil {
			t.Fatalf("failed to write batch: %v", err)
		}
		collector.AddRootTime(time.Since(writeStart))
		batch.Reset()
		batches++

		if collector.ShouldReport() {
			t.Logf("Period Summary (Total Items: %d), metrics: %s",
				collector.totalInjected,
				collector.GetMetricsString())
			collector.ResetWindow()
		}
		if targetBytes > 0 && batches%sizeCheckBatches == 0 {
			size, err := GetDirSize(filepath.Clean(baseDir))
			if err != nil {
				t.Fatalf("failed to calculate database size while checking target: %v", err)
			}
			if size >= targetBytes {
				t.Logf("KV stress target reached at %d items: %s >= %s",
					collector.totalInjected,
					bytesToReadable(uint64(size)),
					bytesToReadable(uint64(targetBytes)))
				break
			}
		}
	}
	t.Logf("KV stress completed, total time: %v", time.Since(totalStart))

	if err := ldb.Compact(nil, nil); err != nil {
		t.Fatalf("failed to compact database: %v", err)
	}
	sizeAfterCompact, err := GetDirSize(filepath.Clean(baseDir))
	if err != nil {
		t.Fatalf("failed to calculate compacted database size: %v", err)
	}
	t.Logf("KV stress compacted size: %s", bytesToReadable(uint64(sizeAfterCompact)))
}
