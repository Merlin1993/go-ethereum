package tree

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/ethdb"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	trieutils "github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
)

// Test configuration
const (
	verkleDir = "F:\\trie_stress_data\\verkle"
)

var (
	verkleStressItems      = flag.Int("verkleStressItems", method1TotalData, "Total items to inject in TestTrieStressVerkle")
	verkleStressBatchSize  = flag.Int("verkleStressBatchSize", method1BatchSize, "Items per commit batch in TestTrieStressVerkle")
	verkleStressEpochItems = flag.Int("verkleStressEpochItems", 1000000, "Items per metrics window in TestTrieStressVerkle")
	verkleStressBaseDir    = flag.String("verkleStressBaseDir", verkleDir, "Base directory for TestTrieStressVerkle")
)

// TestTrieStressVerkle: batch writes and commit with sliding window updates
func TestTrieStressVerkle(t *testing.T) {
	totalData := *verkleStressItems
	batchPerCommit := *verkleStressBatchSize
	epochItems := *verkleStressEpochItems
	baseDir := *verkleStressBaseDir
	if totalData <= 0 || batchPerCommit <= 0 || epochItems <= 0 {
		t.Fatalf("verkleStressItems, verkleStressBatchSize and verkleStressEpochItems must all be positive")
	}

	// Create temporary directory
	os.RemoveAll(baseDir)
	os.MkdirAll(baseDir, os.ModePerm)

	// Create database
	ldb, err := leveldb.New(baseDir, 128, 128, "verkle-test", false)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer ldb.Close()

	// Create trie database
	cacheConfig := core.DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	mdb := ethdb.WrapWithStats(ldb)
	diskDB := rawdb.NewDatabase(mdb)
	trieDB := triedb.NewDatabase(diskDB, cacheConfig.TriedbConfig(true))

	// Read last root hash
	lastRoot, err := loadLastRoot(diskDB)
	if err != nil {
		t.Fatalf("failed to read last root hash: %v", err)
	}
	t.Logf("read last root hash: %v", lastRoot.String())

	// Create point cache
	pointCache := trieutils.NewPointCache(1024)

	// Create Verkle trie
	vt, err := trie.NewVerkleTrie(lastRoot, trieDB, pointCache)
	if err != nil {
		t.Fatalf("failed to create Verkle trie: %v", err)
	}

	var finalRoot common.Hash = lastRoot
	totalStart := time.Now()

	collector := NewMetricsCollector(epochItems, baseDir, "verkle_stress.csv")
	defer collector.Close()

	// Use a fixed address for testing
	testAddr := common.Address{}

	// Batch write data
	for i := 0; i < totalData; i += batchPerCommit {
		batchSize := batchPerCommit
		if i+batchPerCommit > totalData {
			batchSize = totalData - i
		}

		// 1. Insert new keys (1000 items)
		newKeys := make([][]byte, batchSize)
		for j := 0; j < batchSize; j++ {
			key, value := generateRandomData()
			if err := vt.UpdateStorage(testAddr, key, value); err != nil {
				t.Fatalf("failed to update storage: %v", err)
			}
			newKeys[j] = key
		}

		// 2. Randomly update 1,000 keys from the pool
		updateKeys := collector.GetRandomKeys(batchSize)
		if len(updateKeys) > 0 {
			for _, key := range updateKeys {
				_, val := generateRandomData()
				if err := vt.UpdateStorage(testAddr, key, val); err != nil {
					t.Fatalf("failed to update storage: %v", err)
				}
			}
			collector.AddUpdated(updateKeys)
		}

		collector.AddInjected(batchSize, newKeys)

		// Commit and get root hash
		rootStart := time.Now()
		root, nodes := vt.Commit(false)
		collector.AddRootTime(time.Since(rootStart))

		// Update database
		mergedNodeset := trienode.NewWithNodeSet(nodes)
		stateSet := triedb.NewStateSet()

		if err := trieDB.Update(root, finalRoot, uint64(i), mergedNodeset, stateSet); err != nil {
			t.Fatalf("failed to update database: %v", err)
		}
		if err := trieDB.Commit(root, false); err != nil {
			t.Fatalf("failed to commit database: %v", err)
		}

		// Prune historical state
		trieDB.Cap(0)

		finalRoot = root
		// Save last root hash
		if err := saveLastRoot(diskDB, root); err != nil {
			t.Fatalf("failed to save last root hash: %v", err)
		}

		// Create new Verkle trie and continue writing
		vt, err = trie.NewVerkleTrie(root, trieDB, pointCache)
		if err != nil {
			t.Fatalf("failed to create new Verkle trie: %v", err)
		}
		if collector.ShouldReport() {
			t.Logf("Period Summary (Total Items: %d), metrics: %s", collector.totalInjected, collector.GetMetricsString())
			collector.ResetWindow()
		}
	}

	totalTime := time.Since(totalStart)
	t.Logf("method 1 completed, total elapsed: %v, final root: %x", totalTime, finalRoot)
}

// Method 2: multiple small batches based on existing root
func TestVerkleMethod2(t *testing.T) {
	// Create database
	ldb, err := leveldb.New(verkleDir, 128, 128, "verkle-test", false)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer ldb.Close()

	// Create trie database
	cacheConfig := core.DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	diskDB := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(diskDB, cacheConfig.TriedbConfig(true))

	// Read last root hash
	lastRoot, err := loadLastRoot(diskDB)
	if err != nil {
		t.Fatalf("failed to read last root hash: %v", err)
	}

	// Create point cache
	pointCache := trieutils.NewPointCache(1024)

	// Create Verkle trie
	vt, err := trie.NewVerkleTrie(lastRoot, trieDB, pointCache)
	if err != nil {
		t.Fatalf("failed to create Verkle trie: %v", err)
	}

	// Record duration per step
	var (
		writeTimes  []time.Duration
		commitTimes []time.Duration
		rootTimes   []time.Duration
	)

	// Prepare CSV records
	csvRecords := [][]string{
		{"Iteration", "write time(ns)", "root generation time(ns)", "commit time(ns)", "root hash"},
	}

	// Use a fixed address for testing
	testAddr := common.Address{}

	// Perform multiple small-batch writes
	for i := 0; i < method2Iterations; i++ {
		// Write data
		writeStart := time.Now()
		for j := 0; j < method2BatchSize; j++ {
			key, value := generateRandomData()
			if err := vt.UpdateStorage(testAddr, key, value); err != nil {
				t.Fatalf("failed to update storage: %v", err)
			}
		}
		writeTime := time.Since(writeStart)
		writeTimes = append(writeTimes, writeTime)

		// Generate root hash
		rootStart := time.Now()
		root, nodes := vt.Commit(false)
		rootTime := time.Since(rootStart)
		rootTimes = append(rootTimes, rootTime)

		// Commit to database
		commitStart := time.Now()
		mergedNodeset := trienode.NewWithNodeSet(nodes)
		stateSet := triedb.NewStateSet()

		if err := trieDB.Update(root, lastRoot, 0, mergedNodeset, stateSet); err != nil {
			t.Fatalf("failed to update database: %v", err)
		}
		if err := trieDB.Commit(root, false); err != nil {
			t.Fatalf("failed to commit database: %v", err)
		}
		lastRoot = root

		// Save last root hash
		if err := saveLastRoot(diskDB, root); err != nil {
			t.Fatalf("保存最后一个根哈希失败: %v", err)
		}

		commitTime := time.Since(commitStart)
		commitTimes = append(commitTimes, commitTime)

		// Append to CSV records
		csvRecords = append(csvRecords, []string{
			strconv.Itoa(i + 1),
			strconv.FormatInt(writeTime.Nanoseconds(), 10),
			strconv.FormatInt(rootTime.Nanoseconds(), 10),
			strconv.FormatInt(commitTime.Nanoseconds(), 10),
			root.Hex(),
		})

		// Create new Verkle trie and continue writing
		vt, err = trie.NewVerkleTrie(root, trieDB, pointCache)
		if err != nil {
			t.Fatalf("failed to create new Verkle trie: %v", err)
		}

		t.Logf("iteration %d complete, write: %v, root: %v, commit: %v, root: %x",
			i+1, writeTime, rootTime, commitTime, root)
	}

	// 计算平均耗时
	var avgWrite, avgRoot, avgCommit time.Duration
	for i := 0; i < method2Iterations; i++ {
		avgWrite += writeTimes[i]
		avgRoot += rootTimes[i]
		avgCommit += commitTimes[i]
	}
	avgWrite /= time.Duration(method2Iterations)
	avgRoot /= time.Duration(method2Iterations)
	avgCommit /= time.Duration(method2Iterations)

	t.Logf("method 2 completed, average — write: %v, root: %v, commit: %v",
		avgWrite, avgRoot, avgCommit)

	// 将结果写入CSV文件
	csvFileName := fmt.Sprintf("verkle_method2_batch%d_iter%d.csv", method2BatchSize, method2Iterations)
	writeCSVFile(t, csvFileName, csvRecords)
}

// BenchmarkVT_Update measures Verkle trie insert performance
func BenchmarkVT_Update(b *testing.B) {
	// Create memory database
	memDB := rawdb.NewMemoryDatabase()

	// Create trie database config
	cacheConfig := core.DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	trieDB := triedb.NewDatabase(memDB, cacheConfig.TriedbConfig(true))

	// Create new Verkle trie (no point cache)
	vt, err := trie.NewVerkleTrie(common.Hash{}, trieDB, nil)
	if err != nil {
		b.Fatalf("failed to create new Verkle trie: %v", err)
	}

	// Use a fixed address for testing
	testAddr := common.Address{}

	b.ResetTimer()
	b.ReportAllocs()
	startTime := time.Now()

	for i := 0; i < b.N; i++ {
		key, value := generateRandomData()
		if err := vt.UpdateStorage(testAddr, key, value); err != nil {
			b.Fatalf("failed to update storage: %v", err)
		}
	}

	insertDuration := time.Since(startTime)

	// 获取根哈希
	root, _ := vt.Commit(false)

	b.Logf("inserted %d key-value pairs in: %v (avg: %v), final root: %x",
		b.N, insertDuration, insertDuration/time.Duration(b.N), root)
}
