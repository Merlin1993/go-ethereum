package binary

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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
func TestTrieStress(t *testing.T) {
	// 1. 测试参数
	TargetItems := 500000000 // 总目标量 (5亿)
	EpochItems := 1000000    // 一个统计周期 (100万条)
	BatchSize := 2000        // 每个 Commit 的数据量

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

	// 3. CSV 设置
	csvPath := filepath.Join(baseDir, "asct_stress_test.csv")
	csvFile, err := os.Create(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	header := []string{
		"Injected_Items_Millions",
		"Avg_Root_Calc_Time_ms",
		"Max_Root_Calc_Time_ms",
		"Cumulative_Storage_Bytes",
		"Total_Bucket_Count",
		"Max_Buckets_On_Single_Path",
		"Archived_Storage_Bytes",
		"Total_Archived_Items",
		"Memory_RSS_MB",
		"Memory_Heap_Alloc_MB",
	}
	writer.Write(header)

	// 4. 压力测试循环
	fmt.Printf("开始压力测试: 目标 %d 条\n", TargetItems)

	proc, _ := process.NewProcess(int32(os.Getpid()))
	var mem runtime.MemStats

	totalInjected := 0

	for epoch := 0; totalInjected < TargetItems; epoch++ {
		var (
			epochStartTime = time.Now()
			calcTimes      []time.Duration
			maxCalcTime    time.Duration
		)

		for i := 0; i < EpochItems; i += BatchSize {
			// 模拟随机 Put
			for j := 0; j < BatchSize; j++ {
				key := make([]byte, 32)
				val := make([]byte, 32)
				rand.Read(key)
				rand.Read(val)
				trie.Put(key, val)
			}

			// 触发剪枝 (模拟持续负载下的归档)
			// 每个 batch 剪枝 10 个分片，扫描全树需要约 6500 个 batch (1300万条数据)
			// 这模拟了一个持续但不过于激进的后台归档过程
			//for p := 0; p < 10; p++ {
			trie.PruneNextShard()
			//}

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
		var sumDur time.Duration
		for _, d := range calcTimes {
			sumDur += d
		}
		avgCalcTime := sumDur / time.Duration(len(calcTimes))

		stats := trie.Stats()
		stateSize := getDirSize(stateDir)
		archiveSize := getDirSize(archiveDir)

		proc.MemoryInfo() // 刷新
		memInfo, _ := proc.MemoryInfo()
		runtime.ReadMemStats(&mem)

		// 记录 CSV
		record := []string{
			strconv.FormatFloat(float64(totalInjected)/100000.0, 'f', 1, 64), // 单位：十万条
			fmt.Sprintf("%d", avgCalcTime.Milliseconds()),
			fmt.Sprintf("%d", maxCalcTime.Milliseconds()),
			fmt.Sprintf("%d", stateSize),
			fmt.Sprintf("%d", stats.BucketCount),
			fmt.Sprintf("%d", stats.MaxBucketsPath),
			fmt.Sprintf("%d", archiveSize),
			fmt.Sprintf("%d", stats.ArchivedDataSize),
			fmt.Sprintf("%d", memInfo.RSS/(1024*1024)),
			fmt.Sprintf("%d", mem.HeapAlloc/(1024*1024)),
		}
		writer.Write(record)
		writer.Flush()

		fmt.Printf("周期完成: 已注入 %dM, 耗时 %v, 内存 RSS %dMB, 状态磁盘 %dMB, 归档磁盘 %dMB\n",
			totalInjected/1000000,
			time.Since(epochStartTime),
			memInfo.RSS/(1024*1024),
			stateSize/(1024*1024),
			archiveSize/(1024*1024),
		)
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
