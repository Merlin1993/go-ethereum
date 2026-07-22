package archive

import (
	"bytes"
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

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/shirou/gopsutil/process"
)

var (
	stressItems                     = flag.Int("stressItems", 5242880000, "Total items to inject in TestArchiveTrieStress")
	stressEpochItems                = flag.Int("stressEpochItems", 1000000, "Items per metrics window in TestArchiveTrieStress")
	stressBatchSize                 = flag.Int("stressBatchSize", 1000, "Items per commit batch in TestArchiveTrieStress")
	stressUpdatesPerBatch           = flag.Int("stressUpdatesPerBatch", -1, "Random update operations per commit batch in TestArchiveTrieStress; negative defaults to stressBatchSize")
	stressGetsPerBatch              = flag.Int("stressGetsPerBatch", 1000, "Random Get operations per commit batch in TestArchiveTrieStress")
	stressGetMissPercent            = flag.Int("stressGetMissPercent", 50, "Percent of random Get operations targeting likely non-existent keys")
	stressBaseDir                   = flag.String("stressBaseDir", "F:\\trie_stress_depth16_batch1000_delete_values", "Base directory for TestArchiveTrieStress")
	stressMaxPool                   = flag.Int("stressMaxPool", 10000000, "Maximum sliding key pool size in TestArchiveTrieStress")
	stressShardDepth                = flag.Int("stressShardDepth", 16, "Shard depth for TestArchiveTrieStress; 20 means 1,048,576 shards")
	stressDestructiveCommit         = flag.Bool("stressDestructiveCommit", true, "Unload committed shard nodes during TestArchiveTrieStress commits")
	stressAsyncIO                   = flag.Bool("stressAsyncIO", false, "Pipeline LevelDB batch writes behind the next foreground cycle")
	stressDBBackend                 = flag.String("stressDBBackend", "leveldb", "Database backend for TestArchiveTrieStress: leveldb or pebble")
	stressNodeStorage               = flag.String("stressNodeStorage", NodeStorageHash, "ASC node storage scheme: hash or path")
	stressNodeCacheLimit            = flag.Int("stressNodeCacheLimit", DefaultNodeCacheLimit, "Serialized archive node blob cache entry limit; 0 uses default, negative disables")
	stressNodeCacheBytesLimitMB     = flag.Int("stressNodeCacheBytesLimitMB", 512, "Serialized archive node blob cache byte limit in MiB; 0 uses default, negative disables byte cap")
	stressNodeCacheWarmPathBits     = flag.Int("stressNodeCacheWarmPathBits", DefaultNodeCacheWarmPathBits, "Path-mode eager cache warming depth; 0 uses default, -1 keeps root-only, <-1 disables eager warming")
	stressCommitmentPointCacheLimit = flag.Int("stressCommitmentPointCacheLimit", DefaultCommitmentPointCacheLimit, "Decoded ECMH commitment point cache limit; 0 uses default, negative disables")
	stressArchiveStubMaxBucketsPath = flag.Int("stressArchiveStubMaxBucketsPath", 64, "Max side-mounted archive buckets at one node before pressure-sinking; 0 uses default, negative disables")
	stressPathDiagnostics           = flag.Bool("stressPathDiagnostics", false, "Record path/cache diagnostics during TestArchiveTrieStress")
	stressFullStatsEvery            = flag.Int("stressFullStatsEvery", 5, "Run exact structural stats every N metrics windows in TestArchiveTrieStress; 0 disables exact stats")
	stressFinalStats                = flag.Bool("stressFinalStats", false, "Run one exact structural Stats() pass at the end of TestArchiveTrieStress")
	stressFilterFPSamplesPerBucket  = flag.Int("stressFilterFPSamplesPerBucket", 0, "At the end of TestArchiveTrieStress, sample this many non-member suffixes per archive bucket for Cuckoo false-positive rate; 0 disables")
	stressFilterFPSeed              = flag.Int64("stressFilterFPSeed", 1, "Random seed for archive filter false-positive sampling")
)

type stressAsyncResult struct {
	err error
	dur time.Duration
}

type stressTiming struct {
	root          time.Duration
	wall          time.Duration
	prune         time.Duration
	pruneWait     time.Duration
	pruneShard    time.Duration
	prunePrefetch time.Duration
	insert        time.Duration
	update        time.Duration
	get           time.Duration
	commit        time.Duration
	shard         time.Duration
	rootHash      time.Duration
	write         time.Duration
	rawWrite      time.Duration
	batch         stressBatchStats
}

type stressBatchStats struct {
	bytes       int
	puts        int
	deletes     int
	flatPuts    int
	flatDeletes int
	treePuts    int
	treeDeletes int
}

type stressBatcher struct {
	Batcher
	stats stressBatchStats
}

func newStressBatcher(batch Batcher) *stressBatcher {
	return &stressBatcher{Batcher: batch}
}

func (b *stressBatcher) Put(key, value []byte) error {
	b.stats.puts++
	if bytes.HasPrefix(key, flatValuePrefix) {
		b.stats.flatPuts++
	} else {
		b.stats.treePuts++
	}
	return b.Batcher.Put(key, value)
}

func (b *stressBatcher) Delete(key []byte) error {
	b.stats.deletes++
	if bytes.HasPrefix(key, flatValuePrefix) {
		b.stats.flatDeletes++
	} else {
		b.stats.treeDeletes++
	}
	return b.Batcher.Delete(key)
}

func (b *stressBatcher) snapshot() stressBatchStats {
	stats := b.stats
	stats.bytes = b.ValueSize()
	return stats
}

func (b *stressBatcher) Reset() {
	b.Batcher.Reset()
	b.stats = stressBatchStats{}
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

type stressDBAdapter struct {
	ethdb.KeyValueStore
}

func (db *stressDBAdapter) NewBatch() Batcher {
	return db.KeyValueStore.NewBatch()
}

func (db *stressDBAdapter) PutBucket(hash []byte, data []byte) error {
	return db.Put(hash, data)
}

func (db *stressDBAdapter) GetBucket(hash []byte) ([]byte, error) {
	return db.Get(hash)
}

func (db *stressDBAdapter) DeleteBucket(hash []byte) error {
	return db.Delete(hash)
}

// 内存使用爆炸,查看下是不是哪里有问题.
// TestArchiveTrieStress: Stress test for Archive Trie with sliding window updates
func TestArchiveTrieStress(t *testing.T) {
	// 1. 测试参数
	TargetItems := 1000000000 // 总目标量 (5亿)
	EpochItems := 100000      // 一个统计周期 (10万条)
	BatchSize := 1000         // 每个 Commit 的数据量

	TargetItems = *stressItems
	EpochItems = *stressEpochItems
	BatchSize = *stressBatchSize
	if TargetItems <= 0 || EpochItems <= 0 || BatchSize <= 0 {
		t.Fatalf("stressItems, stressEpochItems and stressBatchSize must all be positive")
	}
	UpdatesPerBatch := *stressUpdatesPerBatch
	if UpdatesPerBatch < 0 {
		UpdatesPerBatch = BatchSize
	}
	if UpdatesPerBatch < 0 {
		t.Fatalf("stressUpdatesPerBatch must be non-negative or -1 for default")
	}
	if *stressShardDepth < 0 || *stressShardDepth > 30 {
		t.Fatalf("stressShardDepth is a bit depth, not a shard count; use 20 for 1,048,576 shards, got %d", *stressShardDepth)
	}
	if *stressGetsPerBatch < 0 || *stressGetMissPercent < 0 || *stressGetMissPercent > 100 {
		t.Fatalf("stressGetsPerBatch must be non-negative and stressGetMissPercent must be in [0,100]")
	}
	if *stressAsyncIO && *stressDestructiveCommit {
		t.Fatalf("stressAsyncIO requires in-memory shards; do not combine it with stressDestructiveCommit")
	}
	if *stressDBBackend != "leveldb" && *stressDBBackend != "pebble" {
		t.Fatalf("stressDBBackend must be leveldb or pebble, got %q", *stressDBBackend)
	}
	if *stressNodeStorage != NodeStorageHash && *stressNodeStorage != NodeStoragePath {
		t.Fatalf("stressNodeStorage must be hash or path, got %q", *stressNodeStorage)
	}

	baseDir := *stressBaseDir
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		t.Fatalf("Failed to create stress base directory %s: %v", baseDir, err)
	}

	// Create fixed directories for the stress test results
	stateDir := filepath.Join(baseDir, "asct_state_db")

	// Try to clean up previous runs
	os.RemoveAll(stateDir)

	if err := os.MkdirAll(stateDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Cleanup at the end (optional, commented out for manual inspection)
	// defer os.RemoveAll(stateDir)

	// 初始化 DB
	var (
		sdb ethdb.KeyValueStore
		err error
	)
	switch *stressDBBackend {
	case "leveldb":
		sdb, err = leveldb.New(stateDir, 512, 256, "state", false)
	case "pebble":
		sdb, err = pebble.New(stateDir, 512, 256, "state", false)
	}
	if err != nil {
		if sdb != nil {
			sdb.Close()
		}
		t.Fatal(err)
	}
	defer sdb.Close()

	// 初始化 Trie
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ShardDepth = *stressShardDepth
	config.NodeStorageScheme = *stressNodeStorage
	config.NodeCacheLimit = *stressNodeCacheLimit
	config.NodeCacheBytesLimit = int64(*stressNodeCacheBytesLimitMB) * 1024 * 1024
	config.NodeCacheWarmPathBits = *stressNodeCacheWarmPathBits
	config.CommitmentPointCacheLimit = *stressCommitmentPointCacheLimit
	config.ArchiveStubMaxBucketsPath = *stressArchiveStubMaxBucketsPath
	config.EnablePathDiagnostics = *stressPathDiagnostics
	trie := NewTrie(nil, &stressDBAdapter{sdb}, hasher, config, true)

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
	csvPath := filepath.Join(resultsDir, "asct_stress_test.csv")
	csvFile, err := os.Create(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	header := []string{
		"Start_Item", "End_Item", "Total_Injected", "Avg_Root_ms", "Avg_LoopWall_ms", "Prune_ms", "Commit_ms", "Write_ms", "Raw_Write_ms", "Max_Root_ms",
		"P95_ms", "P99_ms", "Min_ms", "Q1_ms", "Median_ms", "Q3_ms",
		"State_MB", "Leaf_Count", "Archive_Items", "Bucket_Count", "Max_Buckets_Path", "Bucket_Items_Avg", "Bucket_Items_P50", "Bucket_Items_P95", "Bucket_Items_P99", "Bucket_Items_Max", "RSS_MB", "Heap_MB",
		"Insert_ms", "Update_ms", "Get_ms", "ShardCommit_ms", "RootHash_ms", "Untracked_ms",
		"Batch_KB", "Batch_Puts", "Batch_Deletes", "Flat_Puts", "Flat_Deletes", "Tree_Puts", "Tree_Deletes",
		"FalsePositive_Count", "FalsePositive_Rate",
		"Stats_ms", "Stats_Mode", "NodeCache_Hits", "NodeCache_Misses", "PathNode_DBGets", "Promotion_Checks", "Promotion_Hits", "Bucket_Recomputes",
		"Prune_Wait_ms", "Prune_Shard_ms", "Prune_PrefetchStart_ms", "Prune_Other_ms", "CommitmentPointCache_Hits", "CommitmentPointCache_Misses",
		"Prune_Internal_Visits", "Prune_Hot_Skips", "Prune_Child_Hits", "Prune_Child_Skips", "Prune_Bulk_Collects",
		"Prune_Collected_Leaves", "Prune_Collected_Stubs", "Prune_Build_Items", "Prune_Build_Buckets", "Prune_ArchiveBuild_Parallel",
		"Max_Prune_Shard_ID", "Max_Prune_Shard_ms", "Max_Prune_Shard_Total_ms", "Max_Prune_Shard_Internal_Visits",
		"Max_Prune_Shard_Leaves", "Max_Prune_Shard_Stubs", "Max_Prune_Shard_Build_Items", "Max_Prune_Shard_Build_Buckets",
	}
	writer.Write(header)
	ResetCommitDiagnostics()
	ResetPrunePressureDiagnostics()

	// 4. 压力测试循环
	fmt.Printf("开始压力测试: 目标 %d 条\n", TargetItems)

	proc, _ := process.NewProcess(int32(os.Getpid()))
	var mem runtime.MemStats

	totalInjected := 0
	batch := newStressBatcher(trie.db.NewBatch()) // Assuming trie.db is the KVStore for state
	defer batch.Reset()
	statsEvery := *stressFullStatsEvery
	var lastStats *TrieStats
	var prevDiag CommitDiagnostics
	var prevFalsePositiveCount int64
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
		var calcTimes []stressTiming

		for i := 0; i < EpochItems; i += BatchSize {
			loopStart := time.Now()

			// 触发剪枝 (模拟持续负载下的归档)
			startPrune := time.Now()
			if err := trie.PruneNextShard(); err != nil {
				t.Fatalf("Failed to prune shard: %v", err)
			}
			pruneDur := time.Since(startPrune)

			// 1. Insert new keys (BatchSize items)
			startInsert := time.Now()
			inserts := make([]KeyValue, BatchSize)
			for j := 0; j < BatchSize; j++ {
				key := make([]byte, 32)
				val := make([]byte, 32)
				rand.Read(key)
				rand.Read(val)
				inserts[j] = KeyValue{Key: key, Value: val}

				// Maintain pool with LRU-like FIFO
				if len(keyPool) < maxPool {
					keyPool = append(keyPool, key)
				} else {
					keyPool[poolIndex] = key
					poolIndex = (poolIndex + 1) % maxPool
				}
			}
			if err := trie.PutBatch(inserts); err != nil {
				t.Fatalf("Failed to insert batch: %v", err)
			}
			insertDur := time.Since(startInsert)

			// 2. Randomly update keys from the pool. Set stressUpdatesPerBatch=0
			// for pure append-only insertion workloads.
			startUpdate := time.Now()
			if len(keyPool) > 0 && UpdatesPerBatch > 0 {
				updates := make([]KeyValue, UpdatesPerBatch)
				for j := 0; j < UpdatesPerBatch; j++ {
					key := keyPool[rand.Intn(len(keyPool))]
					val := make([]byte, 32)
					rand.Read(val)
					updates[j] = KeyValue{Key: key, Value: val}

					// Refresh in pool (simplified LRU: just put it back at current index or ignore)
					// To be consistent with MPT/Verkle's AddUpdated, we just put it back at poolIndex
					keyPool[poolIndex] = key
					poolIndex = (poolIndex + 1) % maxPool
				}
				if err := trie.PutBatch(updates); err != nil {
					t.Fatalf("Failed to update batch: %v", err)
				}
			}
			updateDur := time.Since(startUpdate)

			startGet := time.Now()
			for j := 0; j < *stressGetsPerBatch; j++ {
				key := make([]byte, 32)
				if len(keyPool) > 0 && rand.Intn(100) >= *stressGetMissPercent {
					key = keyPool[rand.Intn(len(keyPool))]
				} else {
					rand.Read(key)
				}
				_, _ = trie.Get(key)
			}
			getDur := time.Since(startGet)

			// 计算根耗时统计
			startCommit := time.Now()
			if _, err := trie.CommitToBatch(batch, *stressDestructiveCommit); err != nil {
				t.Fatalf("Failed to commit trie: %v", err)
			}
			commitDur := time.Since(startCommit)
			diag := LastCommitDiagnostics()
			shardCommitDur := time.Duration(diag.ShardCommitNanos)
			rootHashDur := time.Duration(diag.RootHashNanos)

			var rawWriteDur, writeDur time.Duration
			batchStats := batch.snapshot()
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
				batch = newStressBatcher(trie.db.NewBatch())
			} else {
				startWrite := time.Now()
				if err := batch.Write(); err != nil {
					t.Fatalf("Failed to write batch: %v", err)
				}
				batch.Reset()
				writeDur = time.Since(startWrite)
				rawWriteDur = writeDur
			}

			rootDur := pruneDur + commitDur
			wallDur := time.Since(loopStart)

			calcTimes = append(calcTimes, stressTiming{
				root:          rootDur,
				wall:          wallDur,
				prune:         pruneDur,
				pruneWait:     time.Duration(diag.PruneWaitNanos),
				pruneShard:    time.Duration(diag.PruneShardNanos),
				prunePrefetch: time.Duration(diag.PrunePrefetchNanos),
				insert:        insertDur,
				update:        updateDur,
				get:           getDur,
				commit:        commitDur,
				shard:         shardCommitDur,
				rootHash:      rootHashDur,
				write:         writeDur,
				rawWrite:      rawWriteDur,
				batch:         batchStats,
			})

			totalInjected += BatchSize
		}

		// 采集周期指标
		if len(calcTimes) == 0 {
			continue
		}

		fTimes := make([]float64, len(calcTimes))
		var sumRoot, sumPrune, sumPruneWait, sumPruneShard, sumPrunePrefetch float64
		var sumInsert, sumUpdate, sumGet, sumCommit, sumShard, sumRootHash float64
		var sumWall, sumWrite, sumRawWrite float64
		var sumBatchBytes, sumBatchPuts, sumBatchDeletes, sumFlatPuts, sumFlatDeletes, sumTreePuts, sumTreeDeletes int64
		for idx, d := range calcTimes {
			val := float64(d.root.Nanoseconds()) / 1000000.0
			fTimes[idx] = val
			sumRoot += val
			sumWall += float64(d.wall.Nanoseconds()) / 1000000.0
			sumPrune += float64(d.prune.Nanoseconds()) / 1000000.0
			sumPruneWait += float64(d.pruneWait.Nanoseconds()) / 1000000.0
			sumPruneShard += float64(d.pruneShard.Nanoseconds()) / 1000000.0
			sumPrunePrefetch += float64(d.prunePrefetch.Nanoseconds()) / 1000000.0
			sumInsert += float64(d.insert.Nanoseconds()) / 1000000.0
			sumUpdate += float64(d.update.Nanoseconds()) / 1000000.0
			sumGet += float64(d.get.Nanoseconds()) / 1000000.0
			sumCommit += float64(d.commit.Nanoseconds()) / 1000000.0
			sumShard += float64(d.shard.Nanoseconds()) / 1000000.0
			sumRootHash += float64(d.rootHash.Nanoseconds()) / 1000000.0
			sumWrite += float64(d.write.Nanoseconds()) / 1000000.0
			sumRawWrite += float64(d.rawWrite.Nanoseconds()) / 1000000.0
			sumBatchBytes += int64(d.batch.bytes)
			sumBatchPuts += int64(d.batch.puts)
			sumBatchDeletes += int64(d.batch.deletes)
			sumFlatPuts += int64(d.batch.flatPuts)
			sumFlatDeletes += int64(d.batch.flatDeletes)
			sumTreePuts += int64(d.batch.treePuts)
			sumTreeDeletes += int64(d.batch.treeDeletes)
		}
		sort.Float64s(fTimes)
		n := len(fTimes)
		getPercentile := func(p float64) float64 {
			idx := int(p * float64(n-1))
			return fTimes[idx]
		}

		avgRoot := sumRoot / float64(n)
		avgWall := sumWall / float64(n)
		avgPrune := sumPrune / float64(n)
		avgPruneWait := sumPruneWait / float64(n)
		avgPruneShardOnly := sumPruneShard / float64(n)
		avgPrunePrefetch := sumPrunePrefetch / float64(n)
		avgPruneOther := avgPrune - avgPruneWait - avgPruneShardOnly - avgPrunePrefetch
		if avgPruneOther < 0 {
			avgPruneOther = 0
		}
		avgInsert := sumInsert / float64(n)
		avgUpdate := sumUpdate / float64(n)
		avgGet := sumGet / float64(n)
		avgCommit := sumCommit / float64(n)
		avgShard := sumShard / float64(n)
		avgRootHash := sumRootHash / float64(n)
		avgWrite := sumWrite / float64(n)
		avgRawWrite := sumRawWrite / float64(n)
		avgBatchKB := float64(sumBatchBytes) / float64(n) / 1024.0
		avgBatchPuts := float64(sumBatchPuts) / float64(n)
		avgBatchDeletes := float64(sumBatchDeletes) / float64(n)
		avgFlatPuts := float64(sumFlatPuts) / float64(n)
		avgFlatDeletes := float64(sumFlatDeletes) / float64(n)
		avgTreePuts := float64(sumTreePuts) / float64(n)
		avgTreeDeletes := float64(sumTreeDeletes) / float64(n)
		avgTrackedWall := avgPrune + avgInsert + avgUpdate + avgGet + avgCommit + avgWrite
		avgUntracked := avgWall - avgTrackedWall
		if avgUntracked < 0 {
			avgUntracked = 0
		}

		p95 := getPercentile(0.95)
		p99 := getPercentile(0.99)
		minVal := fTimes[0]
		maxVal := fTimes[n-1]
		q1 := getPercentile(0.25)
		median := getPercentile(0.50)
		q3 := getPercentile(0.75)

		stateSize := getDirSize(stateDir)
		proc.MemoryInfo() // 刷新
		memInfo, _ := proc.MemoryInfo()
		runtime.ReadMemStats(&mem)
		statsStart := time.Now()
		statsMode := "disabled"
		if statsEvery > 0 {
			statsMode = "cached"
			if lastStats == nil || epoch%statsEvery == 0 {
				lastStats = trie.Stats()
				statsMode = "exact"
			}
		}
		statsDur := time.Since(statsStart)
		stats := lastStats
		if stats == nil {
			stats = &TrieStats{}
		}
		falsePositiveDelta := stats.FalsePositiveCount - prevFalsePositiveCount
		if falsePositiveDelta < 0 {
			falsePositiveDelta = 0
		}
		if statsMode == "exact" {
			prevFalsePositiveCount = stats.FalsePositiveCount
		}
		falsePositiveRate := 0.0
		getOps := int64(n * *stressGetsPerBatch)
		if getOps > 0 {
			falsePositiveRate = float64(falsePositiveDelta) / float64(getOps)
		}
		diagNow := LastCommitDiagnostics()
		nodeCacheHits := diagNow.NodeCacheHits - prevDiag.NodeCacheHits
		nodeCacheMisses := diagNow.NodeCacheMisses - prevDiag.NodeCacheMisses
		pathNodeDBGets := diagNow.PathNodeDBGets - prevDiag.PathNodeDBGets
		promotionChecks := diagNow.ArchivePromotionChecks - prevDiag.ArchivePromotionChecks
		promotionHits := diagNow.ArchivePromotionHits - prevDiag.ArchivePromotionHits
		bucketRecomputes := diagNow.BucketRecomputes - prevDiag.BucketRecomputes
		commitmentPointCacheHits := diagNow.CommitmentPointCacheHits - prevDiag.CommitmentPointCacheHits
		commitmentPointCacheMisses := diagNow.CommitmentPointCacheMisses - prevDiag.CommitmentPointCacheMisses
		pruneInternalVisits := diagNow.PruneInternalVisits - prevDiag.PruneInternalVisits
		pruneHotSkips := diagNow.PruneHotSkips - prevDiag.PruneHotSkips
		pruneChildHits := diagNow.PruneChildHits - prevDiag.PruneChildHits
		pruneChildSkips := diagNow.PruneChildSkips - prevDiag.PruneChildSkips
		pruneBulkCollects := diagNow.PruneBulkCollects - prevDiag.PruneBulkCollects
		pruneCollectedLeaves := diagNow.PruneCollectedLeaves - prevDiag.PruneCollectedLeaves
		pruneCollectedStubs := diagNow.PruneCollectedStubs - prevDiag.PruneCollectedStubs
		pruneBuildItems := diagNow.PruneBuildItems - prevDiag.PruneBuildItems
		pruneBuildBuckets := diagNow.PruneBuildBuckets - prevDiag.PruneBuildBuckets
		pruneArchiveBuildParallels := diagNow.PruneArchiveBuildParallels - prevDiag.PruneArchiveBuildParallels
		prunePressure := LastPrunePressureDiagnostics()
		prevDiag = diagNow

		// 归档增长强校验 (Panic Check)
		// 只有在完成两个完整裁剪周期后，归档数据才应该有规模性增长。
		cycleItems := (1 << config.ShardDepth) * BatchSize
		if lastStats != nil && totalInjected > 2*cycleItems {
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
			fmt.Sprintf("%.2f", avgRoot),
			fmt.Sprintf("%.2f", avgWall),
			fmt.Sprintf("%.2f", avgPrune),
			fmt.Sprintf("%.2f", avgCommit),
			fmt.Sprintf("%.2f", avgWrite),
			fmt.Sprintf("%.2f", avgRawWrite),
			fmt.Sprintf("%.2f", maxVal),
			fmt.Sprintf("%.2f", p95),
			fmt.Sprintf("%.2f", p99),
			fmt.Sprintf("%.2f", minVal),
			fmt.Sprintf("%.2f", q1),
			fmt.Sprintf("%.2f", median),
			fmt.Sprintf("%.2f", q3),
			fmt.Sprintf("%d", stateSize/(1024*1024)),
			fmt.Sprintf("%d", stats.LeafCount),
			fmt.Sprintf("%d", stats.ArchivedDataSize),
			fmt.Sprintf("%d", stats.BucketCount),
			fmt.Sprintf("%d", stats.MaxBucketsPath),
			fmt.Sprintf("%.2f", stats.BucketItemsAvg),
			fmt.Sprintf("%d", stats.BucketItemsP50),
			fmt.Sprintf("%d", stats.BucketItemsP95),
			fmt.Sprintf("%d", stats.BucketItemsP99),
			fmt.Sprintf("%d", stats.BucketItemsMax),
			fmt.Sprintf("%d", memInfo.RSS/(1024*1024)),
			fmt.Sprintf("%d", mem.HeapAlloc/(1024*1024)),
			fmt.Sprintf("%.2f", avgInsert),
			fmt.Sprintf("%.2f", avgUpdate),
			fmt.Sprintf("%.2f", avgGet),
			fmt.Sprintf("%.2f", avgShard),
			fmt.Sprintf("%.2f", avgRootHash),
			fmt.Sprintf("%.2f", avgUntracked),
			fmt.Sprintf("%.2f", avgBatchKB),
			fmt.Sprintf("%.2f", avgBatchPuts),
			fmt.Sprintf("%.2f", avgBatchDeletes),
			fmt.Sprintf("%.2f", avgFlatPuts),
			fmt.Sprintf("%.2f", avgFlatDeletes),
			fmt.Sprintf("%.2f", avgTreePuts),
			fmt.Sprintf("%.2f", avgTreeDeletes),
			fmt.Sprintf("%d", falsePositiveDelta),
			fmt.Sprintf("%.8f", falsePositiveRate),
			fmt.Sprintf("%.2f", float64(statsDur.Nanoseconds())/1000000.0),
			statsMode,
			fmt.Sprintf("%d", nodeCacheHits),
			fmt.Sprintf("%d", nodeCacheMisses),
			fmt.Sprintf("%d", pathNodeDBGets),
			fmt.Sprintf("%d", promotionChecks),
			fmt.Sprintf("%d", promotionHits),
			fmt.Sprintf("%d", bucketRecomputes),
			fmt.Sprintf("%.2f", avgPruneWait),
			fmt.Sprintf("%.2f", avgPruneShardOnly),
			fmt.Sprintf("%.2f", avgPrunePrefetch),
			fmt.Sprintf("%.2f", avgPruneOther),
			fmt.Sprintf("%d", commitmentPointCacheHits),
			fmt.Sprintf("%d", commitmentPointCacheMisses),
			fmt.Sprintf("%d", pruneInternalVisits),
			fmt.Sprintf("%d", pruneHotSkips),
			fmt.Sprintf("%d", pruneChildHits),
			fmt.Sprintf("%d", pruneChildSkips),
			fmt.Sprintf("%d", pruneBulkCollects),
			fmt.Sprintf("%d", pruneCollectedLeaves),
			fmt.Sprintf("%d", pruneCollectedStubs),
			fmt.Sprintf("%d", pruneBuildItems),
			fmt.Sprintf("%d", pruneBuildBuckets),
			fmt.Sprintf("%d", pruneArchiveBuildParallels),
			fmt.Sprintf("%d", prunePressure.MaxShardID),
			fmt.Sprintf("%.2f", float64(prunePressure.MaxShardNanos)/float64(time.Millisecond)),
			fmt.Sprintf("%.2f", float64(prunePressure.MaxTotalNanos)/float64(time.Millisecond)),
			fmt.Sprintf("%d", prunePressure.MaxInternalVisits),
			fmt.Sprintf("%d", prunePressure.MaxLeaves),
			fmt.Sprintf("%d", prunePressure.MaxStubs),
			fmt.Sprintf("%d", prunePressure.MaxBuildItems),
			fmt.Sprintf("%d", prunePressure.MaxBuildBuckets),
		}
		writer.Write(record)
		writer.Flush()
		ResetPrunePressureDiagnostics()

		fmt.Printf("TrieStats: LeafCount=%d, ArchiveItems=%d, BucketCount=%d, MaxBucketsPath=%d, BucketItemsAvg=%.2f, BucketItemsP50=%d, BucketItemsP95=%d, BucketItemsP99=%d, BucketItemsMax=%d, FalsePositiveDelta=%d, FalsePositiveRate=%.8f\n",
			stats.LeafCount,
			stats.ArchivedDataSize,
			stats.BucketCount,
			stats.MaxBucketsPath,
			stats.BucketItemsAvg,
			stats.BucketItemsP50,
			stats.BucketItemsP95,
			stats.BucketItemsP99,
			stats.BucketItemsMax,
			falsePositiveDelta,
			falsePositiveRate,
		)
		fmt.Printf("Items: %d - %d (Processed Items Count), metrics: State: %s, Leaves: %d, ArchiveItems: %d, Buckets: %d, MaxBucketsPath: %d, BucketItems[Avg: %.2f, P50: %d, P95: %d, P99: %d, Max: %d], Injected: %.2fM, Pool: %d, Root: %.2fms, Wall: %.2fms (Prune: %.2fms, Commit: %.2fms, WriteWait: %.2fms, RawWrite: %.2fms), PruneSplit[Wait: %.2fms, Shard: %.2fms, PrefetchStart: %.2fms, Other: %.2fms], RootP95: %.2fms, RootP99: %.2fms, RootBox[Min: %.1f, Q1: %.1f, Med: %.1f, Q3: %.1f, Max: %.1f], RSS: %dMB, Heap: %dMB\n",
			startItem, endItem,
			bytesToReadable(uint64(stateSize)),
			stats.LeafCount,
			stats.ArchivedDataSize,
			stats.BucketCount,
			stats.MaxBucketsPath,
			stats.BucketItemsAvg,
			stats.BucketItemsP50,
			stats.BucketItemsP95,
			stats.BucketItemsP99,
			stats.BucketItemsMax,
			float64(totalInjected)/1000000.0,
			len(keyPool),
			avgRoot, avgWall, avgPrune, avgCommit, avgWrite, avgRawWrite,
			avgPruneWait,
			avgPruneShardOnly,
			avgPrunePrefetch,
			avgPruneOther,
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
	if *stressFinalStats {
		finalStatsStart := time.Now()
		finalStats := trie.Stats()
		finalStatsDur := time.Since(finalStatsStart)
		finalStatsPath := filepath.Join(resultsDir, "final_structure_stats.csv")
		finalStatsFile, err := os.Create(finalStatsPath)
		if err != nil {
			t.Fatal(err)
		}
		finalStatsWriter := csv.NewWriter(finalStatsFile)
		_ = finalStatsWriter.Write([]string{
			"Total_Injected",
			"Stats_ms",
			"Leaf_Count",
			"Archive_Items",
			"Bucket_Count",
			"Max_Buckets_Path",
			"Bucket_Items_Avg",
			"Bucket_Items_P50",
			"Bucket_Items_P95",
			"Bucket_Items_P99",
			"Bucket_Items_Max",
			"FalsePositive_Count",
		})
		_ = finalStatsWriter.Write([]string{
			fmt.Sprintf("%d", totalInjected),
			fmt.Sprintf("%.2f", float64(finalStatsDur.Nanoseconds())/1000000.0),
			fmt.Sprintf("%d", finalStats.LeafCount),
			fmt.Sprintf("%d", finalStats.ArchivedDataSize),
			fmt.Sprintf("%d", finalStats.BucketCount),
			fmt.Sprintf("%d", finalStats.MaxBucketsPath),
			fmt.Sprintf("%.2f", finalStats.BucketItemsAvg),
			fmt.Sprintf("%d", finalStats.BucketItemsP50),
			fmt.Sprintf("%d", finalStats.BucketItemsP95),
			fmt.Sprintf("%d", finalStats.BucketItemsP99),
			fmt.Sprintf("%d", finalStats.BucketItemsMax),
			fmt.Sprintf("%d", finalStats.FalsePositiveCount),
		})
		finalStatsWriter.Flush()
		if err := finalStatsFile.Close(); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("FinalStats: LeafCount=%d, ArchiveItems=%d, BucketCount=%d, MaxBucketsPath=%d, BucketItemsAvg=%.2f, BucketItemsP50=%d, BucketItemsP95=%d, BucketItemsP99=%d, BucketItemsMax=%d, StatsTime=%s\n",
			finalStats.LeafCount,
			finalStats.ArchivedDataSize,
			finalStats.BucketCount,
			finalStats.MaxBucketsPath,
			finalStats.BucketItemsAvg,
			finalStats.BucketItemsP50,
			finalStats.BucketItemsP95,
			finalStats.BucketItemsP99,
			finalStats.BucketItemsMax,
			finalStatsDur,
		)
	}
	if *stressFilterFPSamplesPerBucket > 0 {
		sampleStart := time.Now()
		fpStats := trie.SampleArchiveFilterFalsePositives(*stressFilterFPSamplesPerBucket, *stressFilterFPSeed)
		sampleDur := time.Since(sampleStart)
		fpPath := filepath.Join(resultsDir, "archive_filter_fp.csv")
		fpFile, err := os.Create(fpPath)
		if err != nil {
			t.Fatal(err)
		}
		fpWriter := csv.NewWriter(fpFile)
		_ = fpWriter.Write([]string{
			"Samples_Per_Bucket",
			"Bucket_Count",
			"Sampled_Buckets",
			"Samples",
			"False_Positives",
			"False_Positive_Rate",
			"Sample_ms",
		})
		_ = fpWriter.Write([]string{
			fmt.Sprintf("%d", *stressFilterFPSamplesPerBucket),
			fmt.Sprintf("%d", fpStats.BucketCount),
			fmt.Sprintf("%d", fpStats.SampledBuckets),
			fmt.Sprintf("%d", fpStats.Samples),
			fmt.Sprintf("%d", fpStats.FalsePositives),
			fmt.Sprintf("%.10f", fpStats.Rate),
			fmt.Sprintf("%.2f", float64(sampleDur.Nanoseconds())/1000000.0),
		})
		fpWriter.Flush()
		if err := fpFile.Close(); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("ArchiveFilterFP: Buckets=%d, SampledBuckets=%d, Samples=%d, FalsePositives=%d, Rate=%.10f, SampleTime=%s\n",
			fpStats.BucketCount,
			fpStats.SampledBuckets,
			fpStats.Samples,
			fpStats.FalsePositives,
			fpStats.Rate,
			sampleDur,
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
