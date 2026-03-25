package binary

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

	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/shirou/gopsutil/process"
)

// LevelDBAdapter adapts leveldb.Database to KVStore interface
type LevelDBAdapter struct {
	*leveldb.Database
}

func (db *LevelDBAdapter) NewBatch() Batcher {
	return db.Database.NewBatch()
}

func (db *LevelDBAdapter) PutBucket(hash []byte, data []byte) error {
	return db.Put(hash, data)
}

func (db *LevelDBAdapter) GetBucket(hash []byte) ([]byte, error) {
	return db.Get(hash)
}

func (db *LevelDBAdapter) DeleteBucket(hash []byte) error {
	return db.Delete(hash)
}

// 内存使用爆炸,查看下是不是哪里有问题.
// TestTrieStressBinary: Stress test for Binary Trie with sliding window updates
func TestTrieStressBinary(t *testing.T) {
	// 1. 测试参数
	TargetItems := 1000000000 // 总目标量 (5亿)
	EpochItems := 100000      // 一个统计周期 (100万条)
	BatchSize := 1000         // 每个 Commit 的数据量

	baseDir := "F:\\trie_stress_data"
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		t.Fatalf("Failed to create F drive directory: %v. Stress test must run on F: drive.", err)
	}

	// Create fixed directories for the stress test results
	stateDir := filepath.Join(baseDir, "asct_state_db")
	archiveDir := filepath.Join(baseDir, "asct_archive_db")

	// Try to clean up previous runs
	os.RemoveAll(stateDir)
	os.RemoveAll(archiveDir)

	if err := os.MkdirAll(stateDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(archiveDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Cleanup at the end (optional, commented out for manual inspection)
	// defer os.RemoveAll(stateDir)
	// defer os.RemoveAll(archiveDir)

	// 初始化 DB
	sdb, err := leveldb.New(stateDir, 512, 256, "state", false)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close()

	adb, err := leveldb.New(archiveDir, 512, 256, "archive", false)
	if err != nil {
		t.Fatal(err)
	}
	defer adb.Close()

	// 初始化 Trie
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ArchiveDB = &LevelDBAdapter{adb}
	trie := NewTrie(nil, &LevelDBAdapter{sdb}, hasher, config, true)

	// 3. Sliding window setup
	maxPool := 10000000 // 10M keys
	keyPool := make([][]byte, 0, maxPool)
	poolIndex := 0

	// 4. CSV 设置
	resultsDir := filepath.Join(baseDir, "results")
	os.MkdirAll(resultsDir, 0755)
	csvPath := filepath.Join(resultsDir, "binary_stress.csv")
	csvFile, err := os.Create(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	header := []string{
		"Start_Item", "End_Item", "Total_Injected", "Avg_Root_ms", "Max_Root_ms",
		"P95_ms", "P99_ms", "Min_ms", "Q1_ms", "Median_ms", "Q3_ms",
		"State_MB", "Archive_MB", "RSS_MB", "Heap_MB",
	}
	writer.Write(header)

	// 4. 压力测试循环
	fmt.Printf("开始压力测试: 目标 %d 条\n", TargetItems)

	proc, _ := process.NewProcess(int32(os.Getpid()))
	var mem runtime.MemStats

	totalInjected := 0

	for epoch := 0; totalInjected < TargetItems; epoch++ {
		var (
			calcTimes   []time.Duration
			maxCalcTime time.Duration
		)

		for i := 0; i < EpochItems; i += BatchSize {
			// 1. Insert new keys (BatchSize items)
			newKeys := make([][]byte, BatchSize)
			for j := 0; j < BatchSize; j++ {
				key := make([]byte, 32)
				val := make([]byte, 32)
				rand.Read(key)
				rand.Read(val)
				trie.Put(key, val)
				newKeys[j] = key

				// Maintain pool with LRU-like FIFO
				if len(keyPool) < maxPool {
					keyPool = append(keyPool, key)
				} else {
					keyPool[poolIndex] = key
					poolIndex = (poolIndex + 1) % maxPool
				}
			}

			// 2. Randomly update BatchSize keys from the pool (1:1 ratio)
			if len(keyPool) > 0 {
				for j := 0; j < BatchSize; j++ {
					key := keyPool[rand.Intn(len(keyPool))]
					val := make([]byte, 32)
					rand.Read(val)
					trie.Put(key, val)

					// Refresh in pool (simplified LRU: just put it back at current index or ignore)
					// To be consistent with MPT/Verkle's AddUpdated, we just put it back at poolIndex
					keyPool[poolIndex] = key
					poolIndex = (poolIndex + 1) % maxPool
				}
			}

			// 触发剪枝 (模拟持续负载下的归档)
			trie.PruneNextShard()

			// 计算根耗时统计
			startCommit := time.Now()
			trie.Commit()
			trie.FlushArchives()
			dur := time.Since(startCommit)

			calcTimes = append(calcTimes, dur)
			if dur > maxCalcTime {
				maxCalcTime = dur
			}

			totalInjected += BatchSize
		}

		// 采集周期指标
		if len(calcTimes) == 0 {
			continue
		}

		fTimes := make([]float64, len(calcTimes))
		var sumDur float64
		for idx, d := range calcTimes {
			val := float64(d.Nanoseconds()) / 1000000.0
			fTimes[idx] = val
			sumDur += val
		}
		sort.Float64s(fTimes)
		n := len(fTimes)
		getPercentile := func(p float64) float64 {
			idx := int(p * float64(n-1))
			return fTimes[idx]
		}

		avgCalc := sumDur / float64(n)
		p95 := getPercentile(0.95)
		p99 := getPercentile(0.99)
		minVal := fTimes[0]
		maxVal := fTimes[n-1]
		q1 := getPercentile(0.25)
		median := getPercentile(0.50)
		q3 := getPercentile(0.75)

		stateSize := getDirSize(stateDir)
		archiveSize := getDirSize(archiveDir)
		proc.MemoryInfo() // 刷新
		memInfo, _ := proc.MemoryInfo()
		runtime.ReadMemStats(&mem)

		// 记录 CSV
		startItem := totalInjected - EpochItems
		endItem := totalInjected - 1
		record := []string{
			fmt.Sprintf("%d", startItem),
			fmt.Sprintf("%d", endItem),
			fmt.Sprintf("%d", totalInjected),
			fmt.Sprintf("%.2f", avgCalc),
			fmt.Sprintf("%.2f", maxVal),
			fmt.Sprintf("%.2f", p95),
			fmt.Sprintf("%.2f", p99),
			fmt.Sprintf("%.2f", minVal),
			fmt.Sprintf("%.2f", q1),
			fmt.Sprintf("%.2f", median),
			fmt.Sprintf("%.2f", q3),
			fmt.Sprintf("%d", stateSize/(1024*1024)),
			fmt.Sprintf("%d", archiveSize/(1024*1024)),
			fmt.Sprintf("%d", memInfo.RSS/(1024*1024)),
			fmt.Sprintf("%d", mem.HeapAlloc/(1024*1024)),
		}
		writer.Write(record)
		writer.Flush()

		fmt.Printf("Items: %d - %d (Processed Items Count), metrics: State: %s, Archive: %s, Injected: %.2fM, Pool: %d, Avg: %.2fms, P95: %.2fms, P99: %.2fms, Box[Min: %.1f, Q1: %.1f, Med: %.1f, Q3: %.1f, Max: %.1f], RSS: %dMB, Heap: %dMB\n",
			startItem, endItem,
			bytesToReadable(uint64(stateSize)),
			bytesToReadable(uint64(archiveSize)),
			float64(totalInjected)/1000000.0,
			len(keyPool),
			avgCalc,
			p95,
			p99,
			minVal,
			q1,
			median,
			q3,
			maxVal,
			memInfo.RSS/(1024*1024),
			mem.HeapAlloc/(1024*1024),
		)
	}
}

// Helper to match MPT/Verkle output formatting
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

// 计算目录大小
func getDirSize(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // 遍历错误忽略
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}

// 生成随机整数 [0, n)
func randInt(n int) int {
	b := make([]byte, 4)
	rand.Read(b)
	// 简单掩码与取模
	val := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	return int(val % uint32(n))
}
