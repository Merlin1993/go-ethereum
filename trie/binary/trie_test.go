package binary

import (
	"encoding/csv"
	"flag"
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

var (
	stressItems                 = flag.Int("stressItems", 1000000000, "Total items to inject in TestTrieStressBinary")
	stressEpochItems            = flag.Int("stressEpochItems", 100000, "Items per metrics window in TestTrieStressBinary")
	stressBatchSize             = flag.Int("stressBatchSize", 1000, "Items per commit batch in TestTrieStressBinary")
	stressBaseDir               = flag.String("stressBaseDir", "F:\\trie_stress_data_final_v3", "Base directory for TestTrieStressBinary")
	stressMaxPool               = flag.Int("stressMaxPool", 10000000, "Maximum sliding key pool size in TestTrieStressBinary")
	stressShardDepth            = flag.Int("stressShardDepth", 8, "Shard depth for TestTrieStressBinary")
	stressArchiveItemCacheLimit = flag.Int("stressArchiveItemCacheLimit", 0, "Decoded archive item cache limit for TestTrieStressBinary; 0 disables item caching, negative keeps all")
	stressDestructiveCommit     = flag.Bool("stressDestructiveCommit", false, "Unload committed shard nodes during TestTrieStressBinary commits")
	stressAsyncIO               = flag.Bool("stressAsyncIO", false, "Pipeline archive flush and LevelDB batch writes behind the next foreground cycle")
)

type stressAsyncResult struct {
	err error
	dur time.Duration
}

type stressTiming struct {
	total    time.Duration
	wall     time.Duration
	prune    time.Duration
	commit   time.Duration
	flush    time.Duration
	write    time.Duration
	rawFlush time.Duration
	rawWrite time.Duration
}

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

	TargetItems = *stressItems
	EpochItems = *stressEpochItems
	BatchSize = *stressBatchSize
	if TargetItems <= 0 || EpochItems <= 0 || BatchSize <= 0 {
		t.Fatalf("stressItems, stressEpochItems and stressBatchSize must all be positive")
	}
	if *stressAsyncIO && *stressDestructiveCommit {
		t.Fatalf("stressAsyncIO requires in-memory shards; do not combine it with stressDestructiveCommit")
	}

	baseDir := *stressBaseDir
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		t.Fatalf("Failed to create stress base directory %s: %v", baseDir, err)
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
	config.ArchiveItemCacheLimit = *stressArchiveItemCacheLimit
	config.ShardDepth = 8 // 降低分片深度以加速裁剪周期触发 (2^8 = 256)
	config.ShardDepth = *stressShardDepth
	config.ArchiveDB = &LevelDBAdapter{adb}
	trie := NewTrie(nil, &LevelDBAdapter{sdb}, hasher, config, true)

	// 3. Sliding window setup
	maxPool := 10000000 // 10M keys
	maxPool = *stressMaxPool
	if maxPool < BatchSize {
		maxPool = BatchSize
	}
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
		"Start_Item", "End_Item", "Total_Injected", "Avg_Root_ms", "Avg_LoopWall_ms", "Prune_ms", "Commit_ms", "Flush_ms", "Write_ms", "Raw_Flush_ms", "Raw_Write_ms", "Max_Root_ms",
		"P95_ms", "P99_ms", "Min_ms", "Q1_ms", "Median_ms", "Q3_ms",
		"State_MB", "Archive_MB", "RSS_MB", "Heap_MB",
	}
	writer.Write(header)

	// 4. 压力测试循环
	fmt.Printf("开始压力测试: 目标 %d 条\n", TargetItems)

	proc, _ := process.NewProcess(int32(os.Getpid()))
	var mem runtime.MemStats

	totalInjected := 0
	batch := trie.db.NewBatch() // Assuming trie.db is the KVStore for state
	defer batch.Reset()
	var pendingWrite <-chan stressAsyncResult
	startAsync := func(fn func() error) <-chan stressAsyncResult {
		ch := make(chan stressAsyncResult, 1)
		go func() {
			start := time.Now()
			ch <- stressAsyncResult{err: fn(), dur: time.Since(start)}
		}()
		return ch
	}

	for epoch := 0; totalInjected < TargetItems; epoch++ {
		var (
			calcTimes   []stressTiming
			maxCalcTime time.Duration
		)

		for i := 0; i < EpochItems; i += BatchSize {
			loopStart := time.Now()

			// 触发剪枝 (模拟持续负载下的归档)
			startPrune := time.Now()
			trie.PruneNextShard()
			pruneDur := time.Since(startPrune)

			var (
				flushCh     <-chan stressAsyncResult
				rawFlushDur time.Duration
				flushDur    time.Duration
			)
			if *stressAsyncIO {
				flushCh = startAsync(trie.FlushArchives)
			} else {
				startFlush := time.Now()
				if err := trie.FlushArchives(); err != nil {
					t.Fatalf("Failed to flush archives: %v", err)
				}
				flushDur = time.Since(startFlush)
				rawFlushDur = flushDur
			}

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

			// 计算根耗时统计
			startCommit := time.Now()
			trie.CommitToBatch(batch, *stressDestructiveCommit)
			commitDur := time.Since(startCommit)

			if flushCh != nil {
				waitStart := time.Now()
				res := <-flushCh
				flushDur = time.Since(waitStart)
				rawFlushDur = res.dur
				if res.err != nil {
					t.Fatalf("Failed to flush archives: %v", res.err)
				}
			}

			var rawWriteDur, writeDur time.Duration
			if *stressAsyncIO {
				if pendingWrite != nil {
					waitStart := time.Now()
					res := <-pendingWrite
					writeDur = time.Since(waitStart)
					rawWriteDur = res.dur
					if res.err != nil {
						t.Fatalf("Failed to write async batch: %v", res.err)
					}
				}
				writeBatch := batch
				pendingWrite = startAsync(func() error {
					err := writeBatch.Write()
					writeBatch.Reset()
					return err
				})
				batch = trie.db.NewBatch()
			} else {
				startWrite := time.Now()
				if err := batch.Write(); err != nil {
					t.Fatalf("Failed to write batch: %v", err)
				}
				batch.Reset()
				writeDur = time.Since(startWrite)
				rawWriteDur = writeDur
			}

			dur := pruneDur + commitDur + flushDur + writeDur
			wallDur := time.Since(loopStart)

			calcTimes = append(calcTimes, stressTiming{
				total:    dur,
				wall:     wallDur,
				prune:    pruneDur,
				commit:   commitDur,
				flush:    flushDur,
				write:    writeDur,
				rawFlush: rawFlushDur,
				rawWrite: rawWriteDur,
			})
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
		var sumDur, sumPrune, sumCommit, sumFlush float64
		var sumWall, sumWrite, sumRawFlush, sumRawWrite float64
		for idx, d := range calcTimes {
			val := float64(d.total.Nanoseconds()) / 1000000.0
			fTimes[idx] = val
			sumDur += val
			sumWall += float64(d.wall.Nanoseconds()) / 1000000.0
			sumPrune += float64(d.prune.Nanoseconds()) / 1000000.0
			sumCommit += float64(d.commit.Nanoseconds()) / 1000000.0
			sumFlush += float64(d.flush.Nanoseconds()) / 1000000.0
			sumWrite += float64(d.write.Nanoseconds()) / 1000000.0
			sumRawFlush += float64(d.rawFlush.Nanoseconds()) / 1000000.0
			sumRawWrite += float64(d.rawWrite.Nanoseconds()) / 1000000.0
		}
		sort.Float64s(fTimes)
		n := len(fTimes)
		getPercentile := func(p float64) float64 {
			idx := int(p * float64(n-1))
			return fTimes[idx]
		}

		avgCalc := sumDur / float64(n)
		avgWall := sumWall / float64(n)
		avgPrune := sumPrune / float64(n)
		avgCommit := sumCommit / float64(n)
		avgFlush := sumFlush / float64(n)
		avgWrite := sumWrite / float64(n)
		avgRawFlush := sumRawFlush / float64(n)
		avgRawWrite := sumRawWrite / float64(n)

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

		// 归档增长强校验 (Panic Check)
		// 只有在完成两个完整裁剪周期后，归档数据才应该有规模性增长。
		cycleItems := (1 << config.ShardDepth) * BatchSize
		if totalInjected > 2*cycleItems {
			stats := trie.Stats()
			// 在 50% 的更新率下，理论归档期望约为 0.5 * cycleItems。
			// 这里设定 10% 为硬性红线，若低于此值则判定归档逻辑失效。
			minExpected := int64(float64(cycleItems) * 0.1)
			if stats.ArchivedDataSize < minExpected {
				panic(fmt.Sprintf("\n[ARCHIVE FAILURE] 归档增长异常过低!\n"+
					"当前注入总量: %d\n"+
					"裁剪轮询周期: %d (ShardDepth: %d, Batch: %d)\n"+
					"理论最小归档期望: %d\n"+
					"实际物理归档项数: %d (Buckets: %d)\n"+
					"当前 GlobalEpochBit: %d\n"+
					"排查建议: 检查 pruneAndArchive 的裁剪判定、Epoch 翻转位或 StubList 的持久化逻辑。",
					totalInjected, cycleItems, config.ShardDepth, BatchSize, minExpected, stats.ArchivedDataSize, stats.BucketCount, trie.GetGlobalEpochBit()))
			}
		}

		// 记录 CSV
		startItem := totalInjected - EpochItems
		endItem := totalInjected - 1
		record := []string{
			fmt.Sprintf("%d", startItem),
			fmt.Sprintf("%d", endItem),
			fmt.Sprintf("%d", totalInjected),
			fmt.Sprintf("%.2f", avgCalc),
			fmt.Sprintf("%.2f", avgWall),
			fmt.Sprintf("%.2f", avgPrune),
			fmt.Sprintf("%.2f", avgCommit),
			fmt.Sprintf("%.2f", avgFlush),
			fmt.Sprintf("%.2f", avgWrite),
			fmt.Sprintf("%.2f", avgRawFlush),
			fmt.Sprintf("%.2f", avgRawWrite),
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

		fmt.Printf("Items: %d - %d (Processed Items Count), metrics: State: %s, Archive: %s, Injected: %.2fM, Pool: %d, Avg: %.2fms, Wall: %.2fms (Prune: %.2fms, Commit: %.2fms, FlushWait: %.2fms, WriteWait: %.2fms, RawFlush: %.2fms, RawWrite: %.2fms), P95: %.2fms, P99: %.2fms, Box[Min: %.1f, Q1: %.1f, Med: %.1f, Q3: %.1f, Max: %.1f], RSS: %dMB, Heap: %dMB\n",
			startItem, endItem,
			bytesToReadable(uint64(stateSize)),
			bytesToReadable(uint64(archiveSize)),
			float64(totalInjected)/1000000.0,
			len(keyPool),
			avgCalc, avgWall, avgPrune, avgCommit, avgFlush, avgWrite, avgRawFlush, avgRawWrite,
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
	if pendingWrite != nil {
		res := <-pendingWrite
		if res.err != nil {
			t.Fatalf("Failed to write final async batch: %v", res.err)
		}
		pendingWrite = nil
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
