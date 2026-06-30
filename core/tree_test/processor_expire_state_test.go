package tree

import (
	stdbinary "encoding/binary"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"hash"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/params"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/database"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

var globalFPDistribution = make(map[string]int64)

// ProcessorHost encapsulates the environment for state processing experiments.
type ProcessorHost struct {
	db     ethdb.Database
	trieDB *triedb.Database
	sdb    state.Database
	snaps  *snapshot.Tree
	config *ProcessorConfig
}

var (
	dbDir            = flag.String("dbDir2", "F:\\expire_data\\expire_state_db", "Database directory")
	dataDir          = flag.String("dataDir2", "E:\\ethdata", "Input data directory")
	startIdx         = flag.Int("startFileIdx2", 1, "Start file index")
	endIdx           = flag.Int("endFileIdx2", 21, "End file index")
	useVerkle        = flag.Bool("useVerkle2", false, "Enable Verkle trie")
	useBinaryTrie    = flag.Bool("useBinaryTrie2", true, "Enable Binary trie")
	useKV            = flag.Bool("useKV2", false, "Enable no-hash KV state backend")
	useMemory        = flag.Bool("useMemory2", false, "Use in-memory DB")
	binaryArchiveDir = flag.String("binaryArchiveDir2", "F:\\expire_data\\expire_state_db_achive", "Binary trie archive directory")
	metricsDir       = flag.String("metricsDir2", ".", "Directory for mainnet metrics CSV/JSON output")
	statsInterval    = flag.Int("statsInterval2", 100000, "Statistics reporting interval (in blocks)")
	pruneInterval    = flag.Int("pruneInterval", 1, "Blocks between Trie.PruneNextShard() calls")
	maxBlocks        = flag.Int("blocks", 0, "Maximum number of blocks to process during processor or consistency tests (0 = all)")

	// Binary Trie Ablation flags
	shardDepth                  = flag.Int("shardDepth", 8, "Binary trie shard depth")
	archiveBucketSize           = flag.Int("archiveBucketSize", 100, "Binary trie archive bucket size")
	archiveItemCacheLimit       = flag.Int("archiveItemCacheLimit", 0, "Binary trie decoded archive item cache limit; 0 disables item caching, negative keeps all")
	cuckooBuckets               = flag.Int("cuckooBuckets", 16, "Binary trie cuckoo filter buckets")
	cuckooSlots                 = flag.Int("cuckooSlots", 4, "Binary trie cuckoo filter slots")
	binaryNodeCacheLimit        = flag.Int("binaryNodeCacheLimit", 262144, "Binary trie process node cache entry limit; 0 uses default, negative disables cache")
	binaryNodeCacheBytesLimitMB = flag.Int("binaryNodeCacheBytesLimitMB", 512, "Binary trie process node cache byte limit in MiB; 0 uses default, negative disables byte cap")
	binaryPathDiagnostics       = flag.Bool("binaryPathDiagnostics", false, "Enable binary trie path/cache diagnostics")
	binaryPruneShardMetrics     = flag.Bool("binaryPruneShardMetrics", false, "Write per-prune binary shard pressure metrics CSV")
	binaryAsyncPrune            = flag.Bool("binaryAsyncPrune", false, "Run binary shard pruning asynchronously and wait before root commit")
	binaryCommitWorkers         = flag.Int("binaryCommitWorkers", 0, "Max parallel binary shard commit workers; 0 uses binary default cap")
	binaryCommitWatchdog        = flag.Int("binaryCommitWatchdogSec", 0, "Dump goroutines if one binary wrapper commit exceeds this many seconds; 0 disables")
	binaryPhysicalDelete        = flag.Bool("binaryPhysicalDelete", false, "Physically delete obsolete binary trie state nodes from stateDB")
	binaryNodeStorage           = flag.String("binaryNodeStorage", "path", "Binary trie node storage scheme: hash or path")
	maxRootPipelineMs           = flag.Int("maxRootPipelineMs", 0, "Abort if any block root pipeline exceeds this many milliseconds; 0 disables")
	maxHandleDestructMs         = flag.Int("maxHandleDestructionMs", 0, "Abort if any block handleDestruction exceeds this many milliseconds; 0 disables")
	maxPruningMs                = flag.Int("maxPruningMs", 0, "Abort if any binary pruning step exceeds this many milliseconds; 0 disables")
)

func TestMain(m *testing.M) {
	if !flag.Parsed() {
		flag.Parse()
	}
	common.DebugFlag = false // 关闭调试标志
	os.Exit(m.Run())
}

type ProcessorConfig struct {
	DbDir            string
	DataDir          string
	StartFileIdx     int
	EndFileIdx       int
	UseVerkle        bool
	UseBinaryTrie    bool
	UseKV            bool
	UseMemory        bool
	BinaryArchiveDir string
	MetricsDir       string
	StartNum         uint64
	PruneInterval    int
	MaxBlocks        int

	// Ablation params
	ShardDepth                  int
	ArchiveBucketSize           int
	ArchiveItemCacheLimit       int
	CuckooBuckets               int
	CuckooSlots                 int
	BinaryNodeCacheLimit        int
	BinaryNodeCacheBytesLimitMB int
	BinaryPathDiagnostics       bool
	BinaryPruneShardMetrics     bool
	BinaryAsyncPrune            bool
	BinaryCommitWorkers         int
	BinaryCommitWatchdog        int
	BinaryPhysicalDelete        bool
	BinaryNodeStorage           string
	MaxRootPipelineMs           int
	MaxHandleDestructMs         int
	MaxPruningMs                int
}

func NewProcessorHost(cfg *ProcessorConfig) (*ProcessorHost, error) {
	var ldb ethdb.KeyValueStore
	var err error

	if cfg.UseMemory {
		ldb = memorydb.New()
	} else {
		ldb, err = leveldb.New(cfg.DbDir, 1024, 1024, "eth-expire-state-test", false)
		if err != nil {
			return nil, fmt.Errorf("failed to create database: %v", err)
		}
	}

	db := rawdb.NewDatabase(ldb)
	if cfg.UseKV {
		sdb := state.NewKVDatabase(db)
		return &ProcessorHost{
			db:     db,
			trieDB: sdb.TrieDB(),
			sdb:    sdb,
			config: cfg,
		}, nil
	}
	hdb := hashdb.Defaults
	pdb := pathdb.Defaults
	if cfg.UseVerkle || !cfg.UseBinaryTrie && !cfg.UseVerkle {
		hdb = nil
	} else {
		pdb = nil
	}

	trieDB := triedb.NewFixedDatabase(db, &triedb.Config{
		Preimages:        false,
		IsVerkle:         cfg.UseVerkle,
		IsBinary:         cfg.UseBinaryTrie,
		CacheTrie:        false,
		ReadCache:        false,
		StartNum:         cfg.StartNum,
		BinaryArchiveDir: cfg.BinaryArchiveDir,
		BinaryAblationConfig: &database.BinaryConfig{
			ShardDepth:            cfg.ShardDepth,
			ArchiveBucketSize:     cfg.ArchiveBucketSize,
			ArchiveItemCacheLimit: cfg.ArchiveItemCacheLimit,
			CuckooBuckets:         cfg.CuckooBuckets,
			CuckooSlots:           cfg.CuckooSlots,
			NodeCacheLimit:        cfg.BinaryNodeCacheLimit,
			NodeCacheBytesLimit:   int64(cfg.BinaryNodeCacheBytesLimitMB) * 1024 * 1024,
			EnablePathDiagnostics: cfg.BinaryPathDiagnostics,
			AsyncPrune:            cfg.BinaryAsyncPrune,
			CommitWorkers:         cfg.BinaryCommitWorkers,
			CommitWatchdogSeconds: cfg.BinaryCommitWatchdog,
			PhysicalDelete:        cfg.BinaryPhysicalDelete,
			NodeStorageScheme:     cfg.BinaryNodeStorage,
		},
		PathDB: pdb,
		HashDB: hdb,
	})

	firstRootHash := types.EmptyRootHash
	if cfg.UseVerkle || cfg.UseBinaryTrie {
		firstRootHash = common.Hash{}
	}
	var activeSnaps *snapshot.Tree
	activeSnaps, err = snapshot.New(snapshot.Config{CacheSize: 100}, db, trieDB, firstRootHash)
	if err != nil {
		return nil, fmt.Errorf("failed to create state snapshot: %v", err)
	}
	sdb := state.NewDatabase(trieDB, activeSnaps)

	host := &ProcessorHost{
		db:     db,
		trieDB: trieDB,
		sdb:    sdb,
		snaps:  activeSnaps,
		config: cfg,
	}

	return host, nil
}

func (h *ProcessorHost) Close() {
	if h.db != nil {
		h.db.Close()
	}
}

func checkDurationLimit(t *testing.T, label string, block uint64, got time.Duration, limitMs int) {
	t.Helper()
	if limitMs <= 0 {
		return
	}
	limit := time.Duration(limitMs) * time.Millisecond
	if got > limit {
		t.Fatalf("%s exceeded configured limit at block %d: got %v > %v", label, block, got, limit)
	}
}

// Transaction loading is now handled by TransactionStreamer in processor_utils.go

// 增加配置 -- binary-trie的分片数 binary-trie stub桶的大小上限.
// mpt测试模式下,让mpt会剪枝,不保留历史数据.
func TestExpireStateProcessor(t *testing.T) {
	fmt.Println(">>> Starting TestExpireStateProcessor")
	if !flag.Parsed() {
		flag.Parse()
	}
	cfg := &ProcessorConfig{
		DbDir:                       *dbDir,
		DataDir:                     *dataDir,
		StartFileIdx:                *startIdx,
		EndFileIdx:                  *endIdx,
		UseVerkle:                   *useVerkle,
		UseBinaryTrie:               *useBinaryTrie && !*useKV,
		UseKV:                       *useKV,
		UseMemory:                   *useMemory,
		BinaryArchiveDir:            *binaryArchiveDir,
		MetricsDir:                  *metricsDir,
		StartNum:                    46147,
		PruneInterval:               *pruneInterval,
		MaxBlocks:                   *maxBlocks,
		ShardDepth:                  *shardDepth,
		ArchiveBucketSize:           *archiveBucketSize,
		ArchiveItemCacheLimit:       *archiveItemCacheLimit,
		CuckooBuckets:               *cuckooBuckets,
		CuckooSlots:                 *cuckooSlots,
		BinaryNodeCacheLimit:        *binaryNodeCacheLimit,
		BinaryNodeCacheBytesLimitMB: *binaryNodeCacheBytesLimitMB,
		BinaryPathDiagnostics:       *binaryPathDiagnostics,
		BinaryPruneShardMetrics:     *binaryPruneShardMetrics,
		BinaryAsyncPrune:            *binaryAsyncPrune,
		BinaryCommitWorkers:         *binaryCommitWorkers,
		BinaryCommitWatchdog:        *binaryCommitWatchdog,
		BinaryPhysicalDelete:        *binaryPhysicalDelete,
		BinaryNodeStorage:           *binaryNodeStorage,
		MaxRootPipelineMs:           *maxRootPipelineMs,
		MaxHandleDestructMs:         *maxHandleDestructMs,
		MaxPruningMs:                *maxPruningMs,
	}

	common.UseVerkle = cfg.UseVerkle
	if cfg.UseVerkle {
		common.VerkleLayerCount = 128
	}

	host, err := NewProcessorHost(cfg)
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer host.Close()
	state.ResetCacheStats() // 从零开始统计

	files, err := compareFindTransactionFiles(cfg.DataDir)
	if err != nil || len(files) == 0 {
	}
	fmt.Printf("[测试] 找到 %d 个交易文件\n", len(files))

	selectedFiles := files[cfg.StartFileIdx-1 : cfg.EndFileIdx]
	fmt.Printf(">>> Found %d transaction files, selected indices %d to %d (count: %d)\n", len(files), cfg.StartFileIdx, cfg.EndFileIdx, len(selectedFiles))
	// Create genesis block and blockchain
	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{},
	}
	genesis := gspec.MustCommit(host.db, host.trieDB)
	lastStateRoot := genesis.Root()
	fmt.Printf(">>> Genesis block committed, root: %s\n", lastStateRoot.Hex())

	// Statistics tracking
	statsIv := uint64(*statsInterval)
	var (
		intervalBlocks         uint64
		totalProcessedBlocks   uint64
		epochID                uint64
		totalTxTime            time.Duration
		maxTxTime              time.Duration
		totalFinaliseTime      time.Duration
		maxFinaliseTime        time.Duration
		totalCommitTime        time.Duration
		maxCommitTime          time.Duration
		maxCommitBlock         uint64
		totalRootPipelineTime  time.Duration
		maxRootPipelineTime    time.Duration
		maxRootPipelineBlock   uint64
		totalRootComputeTime   time.Duration
		maxRootComputeTime     time.Duration
		totalHandleDestruct    time.Duration
		maxHandleDestruct      time.Duration
		maxHandleDestructBlock uint64
		totalPruneTime         time.Duration
		maxPruneTime           time.Duration
		maxPruneBlock          uint64
		totalArchiveCompute    time.Duration
		maxArchiveCompute      time.Duration
		totalArchiveWait       time.Duration
		maxArchiveWait         time.Duration
		maxProofSizeBlockBlock uint64
		pruneCount             uint64
		totalStorageSize       int64 // Cumulative storage size
		intervalTxCount        uint64
		intervalSuccessTxCount uint64
		globalTxCount          uint64
		globalSuccessTxCount   uint64
	)
	slowCommitDiagThreshold := 400 * time.Millisecond

	outputDir := cfg.MetricsDir
	if outputDir == "" {
		outputDir = "."
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("failed to create metrics directory %s: %v", outputDir, err)
	}

	// CSV file setup
	csvFile, err := os.Create(filepath.Join(outputDir, "asct_mainnet_metrics.csv"))
	if err != nil {
		t.Fatalf("failed to create csv file: %v", err)
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	kvStatsFile, err := os.Create(filepath.Join(outputDir, "kv_block_access_stats.csv"))
	if err != nil {
		t.Fatalf("failed to create kv stats csv file: %v", err)
	}
	defer kvStatsFile.Close()
	kvStatsWriter := csv.NewWriter(kvStatsFile)
	defer kvStatsWriter.Flush()

	var pruneShardWriter *csv.Writer
	var pruneShardFile *os.File
	if cfg.UseBinaryTrie && cfg.BinaryPruneShardMetrics {
		pruneShardFile, err = os.Create(filepath.Join(outputDir, "asct_prune_shard_metrics.csv"))
		if err != nil {
			t.Fatalf("failed to create prune shard metrics csv file: %v", err)
		}
		defer pruneShardFile.Close()
		pruneShardWriter = csv.NewWriter(pruneShardFile)
		defer pruneShardWriter.Flush()
		pruneShardWriter.Write([]string{
			"Block", "Shard_ID",
			"Total_us", "Wait_us", "Shard_us", "Prefetch_us",
			"Internal_Visits", "Hot_Skips", "Child_Hits", "Child_Skips", "Bulk_Collects",
			"Collected_Leaves", "Collected_Stubs", "Build_Items", "Build_Buckets", "ArchiveBuild_Parallel",
		})
	}
	if cfg.UseBinaryTrie {
		archivetrie.ResetPrunePressureDiagnostics()
	}

	// Write CSV Header
	writer.Write([]string{
		"Epoch_ID", "Tree_Type", "Cumulative_Storage_Bytes",
		"State_Storage_Bytes", "Archived_Storage_Bytes",
		"State_Storage_Share_Pct", "Archive_Storage_Share_Pct",
		// 内存相关列用于判断是否真泄漏：Heap 持续增长说明仍有长期引用；
		// NodeCache_MB 顶到上限则说明缓存保护生效，后续可调 bytes limit 做性能/内存折中。
		"Heap_Alloc_MB", "Heap_Sys_MB", "Runtime_Sys_MB", "NodeCache_MB", "NodeCache_Entries",
		"Archive_Bytes_Per_Item", "State_Bytes_Per_Active_Leaf",
		"Trie_Child_Node_Count", "Total_Archived_Items", "Total_Bucket_Count",
		"Max_Buckets_On_Single_Path", "Bucket_Items_Avg", "Bucket_Items_P50", "Bucket_Items_P95", "Bucket_Items_P99", "Bucket_Items_Max",
		"Avg_Finalise_Time_ms", "Max_Finalise_Time_ms",
		"Avg_State_Commit_Time_ms", "Max_State_Commit_Time_ms",
		"Avg_Root_Pipeline_Time_ms", "Max_Root_Pipeline_Time_ms",
		"Avg_Root_Compute_Excl_Archive_Wait_ms", "Max_Root_Compute_Excl_Archive_Wait_ms",
		"Max_State_Commit_Block", "Max_Root_Pipeline_Block",
		"Avg_Handle_Destruction_Time_ms", "Max_Handle_Destruction_Time_ms", "Max_Handle_Destruction_Block",
		"Avg_Pruning_Time_us", "Max_Pruning_Time_us",
		"Avg_Archive_Compute_Time_us", "Max_Archive_Compute_Time_us",
		"Avg_Archive_Wait_Time_us", "Max_Archive_Wait_Time_us",
		"Max_Pruning_Shard_ID", "Max_Pruning_Shard_Time_us", "Max_Pruning_Shard_Total_us",
		"Max_Pruning_Shard_Wait_us", "Max_Pruning_Shard_Prefetch_us",
		"Max_Pruning_Shard_Internal_Visits", "Max_Pruning_Shard_Hot_Skips",
		"Max_Pruning_Shard_Child_Hits", "Max_Pruning_Shard_Child_Skips",
		"Max_Pruning_Shard_Bulk_Collects", "Max_Pruning_Shard_Collected_Leaves",
		"Max_Pruning_Shard_Collected_Stubs", "Max_Pruning_Shard_Build_Items",
		"Max_Pruning_Shard_Build_Buckets", "Max_Pruning_Shard_ArchiveBuild_Parallel",
		"Hit_Count", "Miss_NonExistent_Count", "Miss_Existent_Count",
		"Avg_Proof_Gen_Time_ms", "Max_Proof_Gen_Time_ms", "Avg_Proof_Verify_Time_ms", "Max_Proof_Verify_Time_ms",
		"Avg_Proof_Size_Byte", "Max_Proof_Size_Byte",
		"Max_Pruning_Block", "Max_Proof_Size_Block",
		"Block_Start", "Block_End",
		"Item_Proof_Min", "Item_Proof_P25", "Item_Proof_Med", "Item_Proof_P75", "Item_Proof_Max",
		"Cycle_FP_Count", "Max_FP_In_Single_Block",
	})
	kvStatsWriter.Write([]string{
		"Block",
		"Reads", "Read_3M", "Read_6M", "Read_1Y", "Read_NonExistent",
		"Writes", "Write_3M", "Write_6M", "Write_1Y", "Write_NonExistent",
	})

	reportStats := func() {
		if intervalBlocks == 0 {
			return
		}
		epochID++
		treeType := "MPT"
		if cfg.UseBinaryTrie {
			treeType = "ASCT"
		} else if cfg.UseVerkle {
			treeType = "Verkle"
		} else if cfg.UseKV {
			treeType = "KV"
		}
		stateStorageSize, _ := getDirSize(cfg.DbDir)
		archiveStorageSize := int64(0)
		if cfg.UseBinaryTrie && cfg.BinaryArchiveDir != "" {
			archiveStorageSize, _ = getDirSize(cfg.BinaryArchiveDir)
		}
		totalStorageSize = stateStorageSize + archiveStorageSize

		var (
			trieChildNodeCount int64
			totalArchivedItems int64
			totalBucketCount   int
			maxBucketsPath     int
			bucketItemsAvg     float64
			bucketItemsP50     int
			bucketItemsP95     int
			bucketItemsP99     int
			bucketItemsMax     int
		)
		if cfg.UseBinaryTrie {
			if active := host.trieDB.GetArchiveTrie(); active != nil {
				if bt, ok := active.(*archivetrie.Trie); ok {
					stats := bt.Stats()
					trieChildNodeCount = stats.LeafCount
					totalArchivedItems = stats.ArchivedDataSize
					totalBucketCount = stats.BucketCount
					maxBucketsPath = stats.MaxBucketsPath
					bucketItemsAvg = stats.BucketItemsAvg
					bucketItemsP50 = stats.BucketItemsP50
					bucketItemsP95 = stats.BucketItemsP95
					bucketItemsP99 = stats.BucketItemsP99
					bucketItemsMax = stats.BucketItemsMax
				}
			}
		}
		stateStorageSharePct := 0.0
		archiveStorageSharePct := 0.0
		if totalStorageSize > 0 {
			stateStorageSharePct = float64(stateStorageSize) * 100 / float64(totalStorageSize)
			archiveStorageSharePct = float64(archiveStorageSize) * 100 / float64(totalStorageSize)
		}
		archiveBytesPerItem := 0.0
		if totalArchivedItems > 0 {
			archiveBytesPerItem = float64(archiveStorageSize) / float64(totalArchivedItems)
		}
		stateBytesPerActiveLeaf := 0.0
		if trieChildNodeCount > 0 {
			stateBytesPerActiveLeaf = float64(stateStorageSize) / float64(trieChildNodeCount)
		}
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		var commitDiag archivetrie.CommitDiagnostics
		if cfg.UseBinaryTrie {
			// LastCommitDiagnostics 里带有 wrapper 每次 commit 记录的 node cache 体量。
			commitDiag = archivetrie.LastCommitDiagnostics()
		}

		fmt.Printf("  Blocks: %d - %d (Processed Blocks Count)\n", totalProcessedBlocks-intervalBlocks, totalProcessedBlocks-1)
		fmt.Printf("  Tx Execution   - Avg: %v, Max: %v\n", totalTxTime/time.Duration(intervalBlocks), maxTxTime)
		if intervalTxCount > 0 {
			fmt.Printf("  Tx Success Rate - %.2f%% (%d/%d)\n", float64(intervalSuccessTxCount)*100/float64(intervalTxCount), intervalSuccessTxCount, intervalTxCount)
		}
		fmt.Printf("  Finalise - Avg: %.2f ms, Max: %v\n", float64(totalFinaliseTime.Milliseconds())/float64(intervalBlocks), maxFinaliseTime)
		fmt.Printf("  State Commit - Avg: %.2f ms, Max: %v\n", float64(totalCommitTime.Milliseconds())/float64(intervalBlocks), maxCommitTime)
		fmt.Printf("  Root Pipeline - Avg: %.2f ms, Max: %v\n", float64(totalRootPipelineTime.Milliseconds())/float64(intervalBlocks), maxRootPipelineTime)
		fmt.Printf("  Root Compute excl archive wait - Avg: %.2f ms, Max: %v\n", float64(totalRootComputeTime.Milliseconds())/float64(intervalBlocks), maxRootComputeTime)
		fmt.Printf("  Storage bytes: total=%d, state=%d, archive=%d\n", totalStorageSize, stateStorageSize, archiveStorageSize)
		fmt.Printf("  Storage shares: state=%.2f%%, archive=%.2f%%, archiveBytesPerItem=%.2f, stateBytesPerActiveLeaf=%.2f\n",
			stateStorageSharePct, archiveStorageSharePct, archiveBytesPerItem, stateBytesPerActiveLeaf)
		fmt.Printf("  Memory - HeapAlloc=%dMB HeapSys=%dMB RuntimeSys=%dMB NodeCache=%dMB entries=%d\n",
			memStats.HeapAlloc/(1024*1024),
			memStats.HeapSys/(1024*1024),
			memStats.Sys/(1024*1024),
			commitDiag.NodeCacheBytes/(1024*1024),
			commitDiag.NodeCacheEntries,
		)
		if cfg.UseBinaryTrie {
			fmt.Printf("  ASCT Struct - Leaves=%d, ArchiveItems=%d, Buckets=%d, MaxBucketsPath=%d\n",
				trieChildNodeCount, totalArchivedItems, totalBucketCount, maxBucketsPath)
		}

		avgBinaryPruneTime := 0.0
		avgArchiveCompute := 0.0
		avgArchiveWait := 0.0
		if pruneCount > 0 {
			avgBinaryPruneTime = float64(totalPruneTime) / float64(pruneCount) / float64(time.Microsecond) // ns -> us
			avgArchiveCompute = float64(totalArchiveCompute) / float64(pruneCount) / float64(time.Microsecond)
			avgArchiveWait = float64(totalArchiveWait) / float64(pruneCount) / float64(time.Microsecond)
		}
		var prunePressure archivetrie.PrunePressureDiagnostics
		if cfg.UseBinaryTrie {
			prunePressure = archivetrie.LastPrunePressureDiagnostics()
		}
		if pruneCount > 0 {
			fmt.Printf("  ASCT archive compute - Avg: %.2f us, Max: %v, WaitAvg: %.2f us, WaitMax: %v\n",
				float64(totalArchiveCompute)/float64(pruneCount)/float64(time.Microsecond),
				maxArchiveCompute,
				float64(totalArchiveWait)/float64(pruneCount)/float64(time.Microsecond),
				maxArchiveWait,
			)
		}
		fmt.Printf("  平均二进制裁剪耗时: %.2f us\n", avgBinaryPruneTime)
		fmt.Printf("  最大二进制裁剪耗时: %.2f us\n", float64(maxPruneTime)/float64(time.Microsecond))
		fmt.Printf("  命中热状态次数: %d\n", atomic.LoadInt64(&common.BinaryHitCount))
		fmt.Printf("  未命中且数据不存在次数: %d\n", atomic.LoadInt64(&common.BinaryMissNonExistentCount))
		fmt.Printf("  未命中但数据存在次数: %d\n", atomic.LoadInt64(&common.BinaryMissExistentCount))

		if cfg.UseBinaryTrie {
			fmt.Printf("  ASCT Prune Pressure - MaxShard=%d ShardTime=%v Total=%v Leaves=%d Stubs=%d BuildItems=%d BuildBuckets=%d InternalVisits=%d\n",
				prunePressure.MaxShardID,
				time.Duration(prunePressure.MaxShardNanos),
				time.Duration(prunePressure.MaxTotalNanos),
				prunePressure.MaxLeaves,
				prunePressure.MaxStubs,
				prunePressure.MaxBuildItems,
				prunePressure.MaxBuildBuckets,
				prunePressure.MaxInternalVisits,
			)
		}

		missExistent := atomic.LoadInt64(&common.BinaryMissExistentCount)
		avgGenTime := 0.0
		if missExistent > 0 {
			avgGenTime = (float64(atomic.LoadInt64(&common.BinaryProofGenTime)) / float64(missExistent)) / 1_000_000.0 // ns -> ms
		}
		maxGenTime := float64(atomic.LoadInt64(&common.BinaryProofGenTimeMax)) / 1_000_000.0

		avgVerifTime := 0.0
		if missExistent > 0 {
			avgVerifTime = (float64(atomic.LoadInt64(&common.BinaryProofVerifTime)) / float64(missExistent)) / 1_000_000.0 // ns -> ms
		}
		maxVerifTime := float64(atomic.LoadInt64(&common.BinaryProofVerifTimeMax)) / 1_000_000.0

		fmt.Printf("  平均证明生成耗时: %.4f ms\n", avgGenTime)
		fmt.Printf("  最大证明生成耗时: %.4f ms\n", maxGenTime)
		fmt.Printf("  平均复活验证耗时: %.4f ms\n", avgVerifTime)
		fmt.Printf("  最大复活验证耗时: %.4f ms\n", maxVerifTime)

		// Proof size metrics
		avgProofSizeBlock := 0.0
		if missExistent > 0 {
			avgProofSizeBlock = float64(atomic.LoadInt64(&common.BinaryTotalProofSize)) / float64(missExistent)
		}
		maxProofSizeBlock := atomic.LoadInt64(&common.BinaryBlockProofSizeMax)
		fmt.Printf("  平均归档命中证明大小: %.2f bytes\n", avgProofSizeBlock)
		fmt.Printf("  单区块证明最大大小: %d bytes\n", maxProofSizeBlock)

		// Five-number summary for item proof sizes
		common.BinaryStatsMu.Lock()
		sizes := make([]int64, len(common.BinaryItemProofSizes))
		copy(sizes, common.BinaryItemProofSizes)
		common.BinaryStatsMu.Unlock()

		var minS, p25S, medS, p75S, maxS int64
		if len(sizes) > 0 {
			sort.Slice(sizes, func(i, j int) bool { return sizes[i] < sizes[j] })
			minS = sizes[0]
			maxS = sizes[len(sizes)-1]
			medS = sizes[len(sizes)/2]
			p25S = sizes[len(sizes)/4]
			p75S = sizes[3*len(sizes)/4]
		}
		fmt.Printf("  Item_Proof_Size 五数概括: Min=%d, P25=%d, Median=%d, P75=%d, Max=%d\n", minS, p25S, medS, p75S, maxS)

		fmt.Printf("  假阳性触发次数: %d\n", atomic.LoadInt64(&common.BinaryCycleFPCount))
		fmt.Printf("  单区块假阳性最大次数: %d\n", atomic.LoadInt64(&common.BinaryMaxFPInSingleBlock))

		common.BinaryStatsMu.Lock()
		fpDistCount := len(common.BinaryFPDistribution)
		var fpAvgBucketSize float64
		if fpDistCount > 0 {
			var sum int64
			for _, size := range common.BinaryFPDistribution {
				sum += size
			}
			fpAvgBucketSize = float64(sum) / float64(fpDistCount)
		}
		common.BinaryStatsMu.Unlock()
		fmt.Printf("  假阳性归档桶平均大小: %.2f (样本数: %d)\n", fpAvgBucketSize, fpDistCount)

		// 写入 CSV
		record := []string{
			strconv.FormatUint(epochID, 10),
			treeType,
			strconv.FormatInt(totalStorageSize, 10),
			strconv.FormatInt(stateStorageSize, 10),
			strconv.FormatInt(archiveStorageSize, 10),
			fmt.Sprintf("%.2f", stateStorageSharePct),
			fmt.Sprintf("%.2f", archiveStorageSharePct),
			strconv.FormatUint(memStats.HeapAlloc/(1024*1024), 10),
			strconv.FormatUint(memStats.HeapSys/(1024*1024), 10),
			strconv.FormatUint(memStats.Sys/(1024*1024), 10),
			strconv.FormatInt(commitDiag.NodeCacheBytes/(1024*1024), 10),
			strconv.FormatInt(commitDiag.NodeCacheEntries, 10),
			fmt.Sprintf("%.2f", archiveBytesPerItem),
			fmt.Sprintf("%.2f", stateBytesPerActiveLeaf),
			strconv.FormatInt(trieChildNodeCount, 10),
			strconv.FormatInt(totalArchivedItems, 10),
			strconv.Itoa(totalBucketCount),
			strconv.Itoa(maxBucketsPath),
			fmt.Sprintf("%.2f", bucketItemsAvg),
			strconv.Itoa(bucketItemsP50),
			strconv.Itoa(bucketItemsP95),
			strconv.Itoa(bucketItemsP99),
			strconv.Itoa(bucketItemsMax),
			fmt.Sprintf("%.2f", float64(totalFinaliseTime.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxFinaliseTime.Milliseconds(), 10),
			fmt.Sprintf("%.2f", float64(totalCommitTime.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxCommitTime.Milliseconds(), 10),
			fmt.Sprintf("%.2f", float64(totalRootPipelineTime.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxRootPipelineTime.Milliseconds(), 10),
			fmt.Sprintf("%.2f", float64(totalRootComputeTime.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxRootComputeTime.Milliseconds(), 10),
			strconv.FormatUint(maxCommitBlock, 10),
			strconv.FormatUint(maxRootPipelineBlock, 10),
			fmt.Sprintf("%.2f", float64(totalHandleDestruct.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxHandleDestruct.Milliseconds(), 10),
			strconv.FormatUint(maxHandleDestructBlock, 10),
			fmt.Sprintf("%.2f", avgBinaryPruneTime),
			strconv.FormatInt(maxPruneTime.Microseconds(), 10),
			fmt.Sprintf("%.2f", avgArchiveCompute),
			strconv.FormatInt(maxArchiveCompute.Microseconds(), 10),
			fmt.Sprintf("%.2f", avgArchiveWait),
			strconv.FormatInt(maxArchiveWait.Microseconds(), 10),
			strconv.FormatInt(prunePressure.MaxShardID, 10),
			strconv.FormatInt(prunePressure.MaxShardNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(prunePressure.MaxTotalNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(prunePressure.MaxWaitNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(prunePressure.MaxPrefetchNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(prunePressure.MaxInternalVisits, 10),
			strconv.FormatInt(prunePressure.MaxHotSkips, 10),
			strconv.FormatInt(prunePressure.MaxChildHits, 10),
			strconv.FormatInt(prunePressure.MaxChildSkips, 10),
			strconv.FormatInt(prunePressure.MaxBulkCollects, 10),
			strconv.FormatInt(prunePressure.MaxLeaves, 10),
			strconv.FormatInt(prunePressure.MaxStubs, 10),
			strconv.FormatInt(prunePressure.MaxBuildItems, 10),
			strconv.FormatInt(prunePressure.MaxBuildBuckets, 10),
			strconv.FormatInt(prunePressure.MaxParallelBuilds, 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryHitCount), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryMissNonExistentCount), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryMissExistentCount), 10),
			fmt.Sprintf("%.4f", avgGenTime),
			fmt.Sprintf("%.4f", maxGenTime),
			fmt.Sprintf("%.4f", avgVerifTime),
			fmt.Sprintf("%.4f", maxVerifTime),
			fmt.Sprintf("%.2f", avgProofSizeBlock),
			strconv.FormatInt(maxProofSizeBlock, 10),
			strconv.FormatUint(maxPruneBlock, 10),
			strconv.FormatUint(maxProofSizeBlockBlock, 10),
			strconv.FormatUint(totalProcessedBlocks-intervalBlocks, 10),
			strconv.FormatUint(totalProcessedBlocks-1, 10),
			strconv.FormatInt(minS, 10),
			strconv.FormatInt(p25S, 10),
			strconv.FormatInt(medS, 10),
			strconv.FormatInt(p75S, 10),
			strconv.FormatInt(maxS, 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryCycleFPCount), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryMaxFPInSingleBlock), 10),
		}
		writer.Write(record)
		writer.Flush()

		// 重置统计变量
		intervalBlocks = 0
		totalFinaliseTime = 0
		maxFinaliseTime = 0
		totalCommitTime = 0
		maxCommitTime = 0
		maxCommitBlock = 0
		totalRootPipelineTime = 0
		maxRootPipelineTime = 0
		maxRootPipelineBlock = 0
		totalRootComputeTime = 0
		maxRootComputeTime = 0
		totalHandleDestruct = 0
		maxHandleDestruct = 0
		maxHandleDestructBlock = 0
		totalPruneTime = 0
		maxPruneTime = 0
		maxPruneBlock = 0
		totalArchiveCompute = 0
		maxArchiveCompute = 0
		totalArchiveWait = 0
		maxArchiveWait = 0
		maxProofSizeBlockBlock = 0
		pruneCount = 0
		totalTxTime = 0 // Reset Tx Execution stats
		maxTxTime = 0   // Reset Tx Execution stats
		intervalTxCount = 0
		intervalSuccessTxCount = 0

		atomic.StoreInt64(&common.BinaryHitCount, 0)
		atomic.StoreInt64(&common.BinaryMissNonExistentCount, 0)
		atomic.StoreInt64(&common.BinaryMissExistentCount, 0)
		atomic.StoreInt64(&common.BinaryCycleFPCount, 0)
		atomic.StoreInt64(&common.BinaryMaxFPInSingleBlock, 0)
		atomic.StoreInt64(&common.BinaryProofGenTime, 0)
		atomic.StoreInt64(&common.BinaryProofGenTimeMax, 0)
		atomic.StoreInt64(&common.BinaryProofVerifTime, 0)
		atomic.StoreInt64(&common.BinaryProofVerifTimeMax, 0)
		atomic.StoreInt64(&common.BinaryTotalProofSize, 0)
		atomic.StoreInt64(&common.BinaryBlockProofSizeMax, 0)
		atomic.StoreInt64(&common.BinaryPruneTime, 0)
		atomic.StoreInt64(&common.BinaryPruneTimeMax, 0)
		common.BinaryStatsMu.Lock()
		common.BinaryItemProofSizes = nil
		common.BinaryItemProofSizeMin = 0
		common.BinaryItemProofSizeMax = 0
		common.BinaryStatsMu.Unlock()
		// FP 分布只用于窗口/最终统计。及时写出并清空切片，避免高误报场景下
		// BinaryFPDistribution 自己成为长跑内存增长源。
		flushGlobalFPDistribution(outputDir)
		if cfg.UseBinaryTrie {
			archivetrie.ResetPrunePressureDiagnostics()
			if pruneShardWriter != nil {
				pruneShardWriter.Flush()
			}
		}
	}

	recordKVBlockStats := func(block uint64) {
		if !cfg.UseKV {
			return
		}
		kvdb, ok := host.sdb.(interface{ KVBlockStats() state.KVAccessStats })
		if !ok {
			return
		}
		stats := kvdb.KVBlockStats()
		kvStatsWriter.Write([]string{
			strconv.FormatUint(stats.Block, 10),
			strconv.FormatUint(stats.Reads, 10),
			strconv.FormatUint(stats.Read3M, 10),
			strconv.FormatUint(stats.Read6M, 10),
			strconv.FormatUint(stats.Read1Y, 10),
			strconv.FormatUint(stats.ReadNonExistent, 10),
			strconv.FormatUint(stats.Writes, 10),
			strconv.FormatUint(stats.Write3M, 10),
			strconv.FormatUint(stats.Write6M, 10),
			strconv.FormatUint(stats.Write1Y, 10),
			strconv.FormatUint(stats.WriteNonExistent, 10),
		})
		if block%100000 == 0 {
			kvStatsWriter.Flush()
		}
	}

	recordPruneShardMetrics := func(block uint64) {
		if pruneShardWriter == nil {
			return
		}
		p := archivetrie.LastPrunePressureDiagnostics()
		pruneShardWriter.Write([]string{
			strconv.FormatUint(block, 10),
			strconv.FormatInt(p.LastShardID, 10),
			strconv.FormatInt(p.LastTotalNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(p.LastWaitNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(p.LastShardNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(p.LastPrefetchNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(p.LastInternalVisits, 10),
			strconv.FormatInt(p.LastHotSkips, 10),
			strconv.FormatInt(p.LastChildHits, 10),
			strconv.FormatInt(p.LastChildSkips, 10),
			strconv.FormatInt(p.LastBulkCollects, 10),
			strconv.FormatInt(p.LastLeaves, 10),
			strconv.FormatInt(p.LastStubs, 10),
			strconv.FormatInt(p.LastBuildItems, 10),
			strconv.FormatInt(p.LastBuildBuckets, 10),
			strconv.FormatInt(p.LastParallelBuilds, 10),
		})
		if block%1000 == 0 {
			pruneShardWriter.Flush()
		}
	}
	recordArchiveAfterCommit := func(block uint64, pruned bool, rootDuration time.Duration) time.Duration {
		rootComputeDuration := rootDuration
		if !pruned || !cfg.UseBinaryTrie {
			return rootComputeDuration
		}
		p := archivetrie.LastPrunePressureDiagnostics()
		archiveCompute := time.Duration(p.LastShardNanos)
		archiveWait := time.Duration(p.LastWaitNanos)
		totalArchiveCompute += archiveCompute
		if archiveCompute > maxArchiveCompute {
			maxArchiveCompute = archiveCompute
		}
		totalArchiveWait += archiveWait
		if archiveWait > maxArchiveWait {
			maxArchiveWait = archiveWait
		}
		if cfg.BinaryAsyncPrune && archiveWait > 0 && archiveWait < rootComputeDuration {
			rootComputeDuration -= archiveWait
		}
		recordPruneShardMetrics(block)
		if cfg.BinaryAsyncPrune {
			checkDurationLimit(t, "binary archive compute", block, archiveCompute, cfg.MaxPruningMs)
		}
		return rootComputeDuration
	}

	var (
		currentBlock           uint64
		currentBlockInit       bool
		duplicateBlocksSkipped uint64
		duplicateTxsSkipped    uint64
	)

processFiles:
	for _, file := range selectedFiles {
		t.Logf("Processing file: %s", file)
		ts, err := NewTransactionStreamer(file)
		if err != nil {
			t.Errorf("failed to open transaction streamer for %s: %v", file, err)
			continue
		}
		defer ts.Close()

		// Load block metadata (timestamps and miners) once per file
		fileIdx := compareGetFileIndex(file)
		compareLoadBlockTimestampsFromFile(cfg.DataDir, fileIdx)

		start10k := time.Now()
		// Process blocks sequentially from the streamer. CSV shards may overlap at
		// file boundaries, so keep currentBlock global across files.
		firstBlock, ok := ts.PeekBlockNum()
		if !ok {
			continue // skip empty file
		}
		if !currentBlockInit {
			currentBlock = firstBlock
			currentBlockInit = true
		}

		for {
			targetBlock, ok := ts.PeekBlockNum()
			if !ok {
				break
			}
			if targetBlock < currentBlock {
				msgs, _ := ts.PopBlock(targetBlock)
				duplicateBlocksSkipped++
				duplicateTxsSkipped += uint64(len(msgs))
				if duplicateBlocksSkipped <= 10 || duplicateBlocksSkipped%1000 == 0 {
					fmt.Printf("[dedup] skipped duplicate/out-of-order block %d txs=%d nextExpected=%d totalSkippedBlocks=%d totalSkippedTxs=%d\n",
						targetBlock, len(msgs), currentBlock, duplicateBlocksSkipped, duplicateTxsSkipped)
				}
				continue
			}

			// Process empty blocks between transactions
			for currentBlock < targetBlock {
				if cfg.MaxBlocks > 0 && int(totalProcessedBlocks) >= cfg.MaxBlocks {
					break processFiles
				}
				b := currentBlock

				if b%100000 == 0 {
					fmt.Printf("[测试] 正在处理区块 %d (总计已处理: %d) 耗时: %v... (空块)\n", b, totalProcessedBlocks, time.Since(start10k))
					start10k = time.Now()
				}

				host.trieDB.UpdateBlockNum(b)
				if host.trieDB.CacheTrie() != nil {
					host.trieDB.CacheTrie().SetBlockNum(b)
				}
				if cfg.UseKV {
					host.sdb.SetBlockNum(b)
				}
				statedb, err := state.New(lastStateRoot, host.sdb)
				if err != nil {
					t.Fatalf("failed to create statedb at empty block %d with root %s: %v", b, lastStateRoot.Hex(), err)
				}

				// 0. Reward miner/packer
				miner := compareBlockMiners[b]
				if miner != (common.Address{}) {
					reward, _ := uint256.FromBig(new(big.Int).Mul(big.NewInt(1e6), big.NewInt(1e18))) // 1,000,000 ETH
					AddBalanceSilent(statedb, miner, reward)
				}

				if cfg.UseBinaryTrie {
					archivetrie.ResetCommitDiagnostics()
				}
				prunedThisBlock := false
				if cfg.UseBinaryTrie && cfg.PruneInterval > 0 && b%uint64(cfg.PruneInterval) == 0 {
					pruneStart := time.Now()
					statedb.PruneNextShard()
					pruneDuration := time.Since(pruneStart)
					totalPruneTime += pruneDuration
					if pruneDuration > maxPruneTime {
						maxPruneTime = pruneDuration
						maxPruneBlock = b
					}
					pruneCount++
					prunedThisBlock = true
					checkDurationLimit(t, "binary pruning", b, pruneDuration, cfg.MaxPruningMs)
				}
				rootStart := time.Now()
				finaliseStart := time.Now()
				statedb.Finalise(false)
				finaliseDuration := time.Since(finaliseStart)

				commitStart := time.Now()
				preCommitStart := time.Now()
				if _, _, err := statedb.PreCommit(false); err != nil {
					t.Fatalf("pre-commit failed at empty block %d: %v", b, err)
				}
				preCommitDuration := time.Since(preCommitStart)
				postCommitStart := time.Now()
				h, err := statedb.PostCommit(b, false, false)
				if err != nil {
					t.Fatalf("post-commit failed at empty block %d: %v", b, err)
				}
				postCommitDuration := time.Since(postCommitStart)
				commitDuration := time.Since(commitStart)
				rootDuration := time.Since(rootStart)
				recordKVBlockStats(b)
				if commitDuration >= slowCommitDiagThreshold || statedb.CommitHandleDestruction >= slowCommitDiagThreshold {
					treeLabel := "MPT"
					extra := ""
					if cfg.UseBinaryTrie {
						treeLabel = "ASCT"
						extra = " " + archivetrie.LastCommitDiagnostics().String() + " " + archivetrie.LastPrunePressureDiagnostics().String()
					} else if cfg.UseVerkle {
						treeLabel = "Verkle"
					}
					fmt.Printf("[%s_COMMIT_DIAG] block=%d empty=true commit=%v pre=%v post=%v commitInternal=%v handleDestruction=%v deleteMerge=%v workers=%v afterWorkers=%v buildUpdate=%v codeWrite=%v accountCommit=%v storageCommit=%v snapshotCommit=%v trieDBCommit=%v readerReset=%v root_pipeline=%v%s\n",
						treeLabel, b, commitDuration, preCommitDuration, postCommitDuration, statedb.CommitInternal, statedb.CommitHandleDestruction, statedb.CommitDeleteMerge, statedb.CommitWorkers, statedb.CommitAfterWorkers, statedb.CommitBuildUpdate, statedb.CommitCodeWrite, statedb.AccountCommits, statedb.StorageCommits, statedb.SnapshotCommits, statedb.TrieDBCommits, statedb.CommitReaderReset, rootDuration, extra)
				}
				checkDurationLimit(t, "root pipeline", b, rootDuration, cfg.MaxRootPipelineMs)
				checkDurationLimit(t, "handleDestruction", b, statedb.CommitHandleDestruction, cfg.MaxHandleDestructMs)
				rootComputeDuration := recordArchiveAfterCommit(b, prunedThisBlock, rootDuration)
				lastStateRoot = h

				totalFinaliseTime += finaliseDuration
				if finaliseDuration > maxFinaliseTime {
					maxFinaliseTime = finaliseDuration
				}
				totalCommitTime += commitDuration
				if commitDuration > maxCommitTime {
					maxCommitTime = commitDuration
					maxCommitBlock = b
				}
				totalHandleDestruct += statedb.CommitHandleDestruction
				if statedb.CommitHandleDestruction > maxHandleDestruct {
					maxHandleDestruct = statedb.CommitHandleDestruction
					maxHandleDestructBlock = b
				}
				totalRootPipelineTime += rootDuration
				if rootDuration > maxRootPipelineTime {
					maxRootPipelineTime = rootDuration
					maxRootPipelineBlock = b
				}
				totalRootComputeTime += rootComputeDuration
				if rootComputeDuration > maxRootComputeTime {
					maxRootComputeTime = rootComputeDuration
				}
				if b%1000 == 0 {
					host.trieDB.Commit(h, false)
					if !cfg.UseBinaryTrie && !cfg.UseVerkle {
						// MPT Mode: Prune historical data to save space.
						// hashdb.Cap(0) flushes old nodes.
						host.trieDB.Cap(0)
					}
				}
				intervalBlocks++
				totalProcessedBlocks++
				currentBlock++
				if intervalBlocks >= statsIv {
					reportStats()
				}
			}

			// Process the block with transactions
			if cfg.MaxBlocks > 0 && int(totalProcessedBlocks) >= cfg.MaxBlocks {
				break processFiles
			}
			b := targetBlock

			if b%100000 == 0 {
				fmt.Printf("[测试] 正在处理区块 %d (总计已处理: %d) 耗时: %v...\n", b, totalProcessedBlocks, time.Since(start10k))
				start10k = time.Now()
			}
			msgs, _ := ts.PopBlock(b)
			host.trieDB.UpdateBlockNum(b)
			if host.trieDB.CacheTrie() != nil {
				host.trieDB.CacheTrie().SetBlockNum(b)
			}
			if cfg.UseKV {
				host.sdb.SetBlockNum(b)
			}
			statedb, err := state.New(lastStateRoot, host.sdb)
			if err != nil {
				t.Fatalf("failed to create statedb at block %d with root %s: %v", b, lastStateRoot.Hex(), err)
			}

			// 0. Reward miner/packer
			miner := compareBlockMiners[targetBlock]
			if miner != (common.Address{}) {
				reward, _ := uint256.FromBig(new(big.Int).Mul(big.NewInt(1e6), big.NewInt(1e18))) // 1,000,000 ETH
				AddBalanceSilent(statedb, miner, reward)
			}

			blockCtx := vm.BlockContext{
				CanTransfer: core.CanTransfer,
				Transfer:    core.Transfer,
				GetHash:     func(n uint64) common.Hash { return common.Hash{} },
				Coinbase:    compareBlockMiners[b],
				BlockNumber: new(big.Int).SetUint64(b),
				Time:        b * 15,
				Difficulty:  big.NewInt(1),
				Random:      &common.Hash{},
				GasLimit:    1000000000,
				BaseFee:     big.NewInt(0),
				BlobBaseFee: big.NewInt(0),
			}
			evm := vm.NewEVM(blockCtx, statedb, params.MainnetChainConfig, vm.Config{})

			// Pre-allocate balance for all senders (matches eth_compare_test)
			bigBalance := new(big.Int).Mul(big.NewInt(1e15), big.NewInt(1e18)) // 1M ETH
			balance, _ := uint256.FromBig(bigBalance)
			for _, m := range msgs {
				statedb.SetBalance(m.From, balance, tracing.BalanceChangeUnspecified)
			}

			txStart := time.Now()
			for _, m := range msgs {
				intervalTxCount++
				globalTxCount++
				m.SkipNonceChecks = true
				_, err := core.ApplyMessage(evm, m, new(core.GasPool).AddGas(m.GasLimit))
				if err == nil {
					// Successful if ApplyMessage returns err == nil (matches eth_compare_test criteria)
					intervalSuccessTxCount++
					globalSuccessTxCount++
				} else {
					toStr := "contract-creation"
					if m.To != nil {
						toStr = m.To.Hex()
					}
					if b%1000 == 0 { // Limit logging to avoid overwhelming output
						fmt.Printf("Transaction Reverted: block=%d, sender=%s, nonce=%d, to=%s, res.Err=%v\n", b, m.From.Hex(), m.Nonce, toStr, err)
					}
				}
			}
			txDuration := time.Since(txStart)
			totalTxTime += txDuration
			if txDuration > maxTxTime {
				maxTxTime = txDuration
			}

			if cfg.UseBinaryTrie {
				archivetrie.ResetCommitDiagnostics()
			}
			prunedThisBlock := false
			if cfg.UseBinaryTrie && cfg.PruneInterval > 0 && b%uint64(cfg.PruneInterval) == 0 {
				pruneStart := time.Now()
				statedb.PruneNextShard()
				pruneDuration := time.Since(pruneStart)
				totalPruneTime += pruneDuration
				if pruneDuration > maxPruneTime {
					maxPruneTime = pruneDuration
					maxPruneBlock = b
				}
				pruneCount++
				prunedThisBlock = true
				checkDurationLimit(t, "binary pruning", b, pruneDuration, cfg.MaxPruningMs)
			}

			// 2. State root calculation time statistics
			rootStart := time.Now()
			finaliseStart := time.Now()
			statedb.Finalise(false)
			finaliseDuration := time.Since(finaliseStart)

			commitStart := time.Now()
			preCommitStart := time.Now()
			if _, _, err := statedb.PreCommit(false); err != nil {
				t.Fatalf("pre-commit failed at block %d: %v", b, err)
			}
			preCommitDuration := time.Since(preCommitStart)
			postCommitStart := time.Now()
			h, err := statedb.PostCommit(b, false, false)
			if err != nil {
				t.Fatalf("post-commit failed at block %d: %v", b, err)
			}
			postCommitDuration := time.Since(postCommitStart)
			commitDuration := time.Since(commitStart)

			rootDuration := time.Since(rootStart)
			recordKVBlockStats(b)
			if commitDuration >= slowCommitDiagThreshold || statedb.CommitHandleDestruction >= slowCommitDiagThreshold {
				treeLabel := "MPT"
				extra := ""
				if cfg.UseBinaryTrie {
					treeLabel = "ASCT"
					extra = " " + archivetrie.LastCommitDiagnostics().String() + " " + archivetrie.LastPrunePressureDiagnostics().String()
				} else if cfg.UseVerkle {
					treeLabel = "Verkle"
				}
				fmt.Printf("[%s_COMMIT_DIAG] block=%d empty=false commit=%v pre=%v post=%v commitInternal=%v handleDestruction=%v deleteMerge=%v workers=%v afterWorkers=%v buildUpdate=%v codeWrite=%v accountCommit=%v storageCommit=%v snapshotCommit=%v trieDBCommit=%v readerReset=%v root_pipeline=%v%s\n",
					treeLabel, b, commitDuration, preCommitDuration, postCommitDuration, statedb.CommitInternal, statedb.CommitHandleDestruction, statedb.CommitDeleteMerge, statedb.CommitWorkers, statedb.CommitAfterWorkers, statedb.CommitBuildUpdate, statedb.CommitCodeWrite, statedb.AccountCommits, statedb.StorageCommits, statedb.SnapshotCommits, statedb.TrieDBCommits, statedb.CommitReaderReset, rootDuration, extra)
			}
			checkDurationLimit(t, "root pipeline", b, rootDuration, cfg.MaxRootPipelineMs)
			checkDurationLimit(t, "handleDestruction", b, statedb.CommitHandleDestruction, cfg.MaxHandleDestructMs)
			rootComputeDuration := recordArchiveAfterCommit(b, prunedThisBlock, rootDuration)

			if b%100000 == 0 {
				fmt.Printf("[测试] block %d: finalise=%v, commit=%v, root_pipeline=%v\n",
					b, finaliseDuration, commitDuration, rootDuration)
			}
			lastStateRoot = h

			// 统计单区块假阳性最大次数与证明最大大小
			fpInBlock := atomic.SwapInt64(&common.BinaryTrieFPInBlock, 0)
			for {
				maxFP := atomic.LoadInt64(&common.BinaryMaxFPInSingleBlock)
				if fpInBlock <= maxFP || atomic.CompareAndSwapInt64(&common.BinaryMaxFPInSingleBlock, maxFP, fpInBlock) {
					break
				}
			}

			blockProofSize := atomic.SwapInt64(&common.BinaryBlockProofSize, 0)
			for {
				maxBlockSize := atomic.LoadInt64(&common.BinaryBlockProofSizeMax)
				if blockProofSize <= maxBlockSize {
					break
				}
				if atomic.CompareAndSwapInt64(&common.BinaryBlockProofSizeMax, maxBlockSize, blockProofSize) {
					maxProofSizeBlockBlock = b
					break
				}
			}

			// Optional treeDB commit
			if b%1000 == 0 {
				host.trieDB.Commit(h, false)
				if !cfg.UseBinaryTrie && !cfg.UseVerkle {
					// MPT Mode: Prune historical data.
					host.trieDB.Cap(0)
				}
			}

			totalFinaliseTime += finaliseDuration
			if finaliseDuration > maxFinaliseTime {
				maxFinaliseTime = finaliseDuration
			}
			totalCommitTime += commitDuration
			if commitDuration > maxCommitTime {
				maxCommitTime = commitDuration
				maxCommitBlock = b
			}
			totalHandleDestruct += statedb.CommitHandleDestruction
			if statedb.CommitHandleDestruction > maxHandleDestruct {
				maxHandleDestruct = statedb.CommitHandleDestruction
				maxHandleDestructBlock = b
			}
			totalRootPipelineTime += rootDuration
			if rootDuration > maxRootPipelineTime {
				maxRootPipelineTime = rootDuration
				maxRootPipelineBlock = b
			}
			totalRootComputeTime += rootComputeDuration
			if rootComputeDuration > maxRootComputeTime {
				maxRootComputeTime = rootComputeDuration
			}

			intervalBlocks++
			totalProcessedBlocks++
			currentBlock++
			if intervalBlocks >= statsIv {
				reportStats()
			}
			// 每 10w 区块刷新一次假阳性分布
			if (b+1)%100000 == 0 {
				flushGlobalFPDistribution(outputDir)
			}
		}
	}
	// 最后不足一个周期的统计报告
	if intervalBlocks > 0 {
		reportStats()
	}
	fmt.Printf("\n>>> FINAL TRANSACTION SUCCESS RATE SUMMARY <<<\n")
	if globalTxCount > 0 {
		fmt.Printf("Total Transactions: %d\n", globalTxCount)
		fmt.Printf("Successful Transactions: %d\n", globalSuccessTxCount)
		fmt.Printf("Global Success Rate: %.2f%%\n", float64(globalSuccessTxCount)*100/float64(globalTxCount))
		if float64(globalSuccessTxCount)/float64(globalTxCount) >= 0.95 {
			fmt.Printf("Status: SUCCESS (>= 95%%)\n")
		} else {
			fmt.Printf("Status: WARNING (< 95%%)\n")
		}
	} else {
		fmt.Printf("No transactions were processed.\n")
	}
	fmt.Printf(">>> END SUMMARY <<<\n\n")

	t.Logf("最终状态根: %s", lastStateRoot.String())
	flushGlobalFPDistribution(outputDir) // 结束后强制刷新一次
}

func flushGlobalFPDistribution(outputDir string) {
	common.BinaryStatsMu.Lock()
	dist := make([]int64, len(common.BinaryFPDistribution))
	copy(dist, common.BinaryFPDistribution)
	common.BinaryFPDistribution = nil
	common.BinaryStatsMu.Unlock()

	for _, d := range dist {
		label := "100+"
		if d < 100 {
			if d < 0 {
				d = 0
			}
			label = strconv.FormatInt(d, 10)
		}
		globalFPDistribution[label]++
	}

	if len(globalFPDistribution) == 0 {
		return
	}
	data, err := json.MarshalIndent(globalFPDistribution, "", "  ")
	if err != nil {
		return
	}
	if outputDir == "" {
		outputDir = "."
	}
	_ = os.WriteFile(filepath.Join(outputDir, "global_fp_distribution.json"), data, 0644)
}

// BlockSummary aggregates state changes for a block.
type BlockSummary struct {
	BlockNum   uint64
	StateRoot  common.Hash
	WriteCount int
	WriteHash  common.Hash
}

type consistencyTracer struct {
	prefix   string
	count    int
	hasher   hash.Hash
	blockNum uint64
}

func newConsistencyTracer(prefix string) *consistencyTracer {
	return &consistencyTracer{
		prefix: prefix,
		hasher: crypto.NewKeccakState(),
	}
}

func (t *consistencyTracer) Reset() {
	t.count = 0
	t.hasher.Reset()
}

func (t *consistencyTracer) Hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnBalanceChange: func(addr common.Address, prev, new *big.Int, reason tracing.BalanceChangeReason) {
			t.count++
			t.hasher.Write(addr[:])
			t.hasher.Write(common.LeftPadBytes(new.Bytes(), 32))

			if t.blockNum == 50107 || t.blockNum == 46170 {
				msg := fmt.Sprintf("[%s] Block %d BalanceChange: addr=%s, reason=%d, new=%v\n", t.prefix, t.blockNum, addr.Hex(), reason, new)
				t.logToFile(msg)
			}
		},
		OnNonceChangeV2: func(addr common.Address, prev, new uint64, reason tracing.NonceChangeReason) {
			t.count++
			t.hasher.Write(addr[:])
			var b [8]byte
			stdbinary.BigEndian.PutUint64(b[:], new)
			t.hasher.Write(b[:])

			if t.blockNum == 50107 || t.blockNum == 46170 {
				msg := fmt.Sprintf("[%s] Block %d NonceChange: addr=%s, new=%d, reason=%d\n", t.prefix, t.blockNum, addr.Hex(), new, reason)
				t.logToFile(msg)
			}
		},
		OnCodeChange: func(addr common.Address, prevCodeHash common.Hash, prevCode []byte, codeHash common.Hash, code []byte) {
			t.count++
			t.hasher.Write(addr[:])
			t.hasher.Write(codeHash[:])

			if t.blockNum == 50107 {
				msg := fmt.Sprintf("[%s] Block %d CodeChange: addr=%s, new=%s\n", t.prefix, t.blockNum, addr.Hex(), codeHash.Hex())
				t.logToFile(msg)
			}
		},
		OnStorageChange: func(addr common.Address, slot common.Hash, prev, new common.Hash) {
			t.count++
			t.hasher.Write(addr[:])
			t.hasher.Write(slot[:])
			t.hasher.Write(new[:])

			if t.blockNum == 50107 || t.blockNum == 46170 {
				msg := fmt.Sprintf("[%s] Block %d StorageChange: addr=%s, slot=%s, prev=%s, new=%s\n", t.prefix, t.blockNum, addr.Hex(), slot.Hex(), prev.Hex(), new.Hex())
				t.logToFile(msg)
			}
		},
	}
}

func (t *consistencyTracer) logToFile(msg string) {
	f, _ := os.OpenFile("debug_b50107.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if f != nil {
		f.WriteString(msg)
		f.Close()
	}
}

func (t *consistencyTracer) Summary(blockNum uint64, stateRoot common.Hash) BlockSummary {
	var h common.Hash
	t.hasher.Sum(h[:0])
	return BlockSummary{
		BlockNum:   blockNum,
		StateRoot:  stateRoot,
		WriteCount: t.count,
		WriteHash:  h,
	}
}

func TestBinaryTrieConsistency(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}
	common.DebugFlag = false

	// 1. MPT Host setup
	mptCfg := &ProcessorConfig{
		DbDir:            filepath.Join(os.TempDir(), "mpt_consistency_db"),
		DataDir:          *dataDir,
		StartFileIdx:     *startIdx,
		EndFileIdx:       *endIdx,
		UseVerkle:        false,
		UseBinaryTrie:    false,
		UseMemory:        false,
		BinaryArchiveDir: "",
		StartNum:         46147,
		PruneInterval:    0,
		MaxBlocks:        *maxBlocks,
	}
	os.RemoveAll(mptCfg.DbDir)
	defer os.RemoveAll(mptCfg.DbDir)
	mptHost, err := NewProcessorHost(mptCfg)
	if err != nil {
		t.Fatalf("failed to create MPT host: %v", err)
	}
	defer mptHost.Close()

	// 2. Binary Trie Host setup
	binCfg := &ProcessorConfig{
		DbDir:                       filepath.Join(os.TempDir(), "bin_consistency_db"),
		DataDir:                     *dataDir,
		StartFileIdx:                *startIdx,
		EndFileIdx:                  *endIdx,
		UseVerkle:                   false,
		UseBinaryTrie:               true,
		UseMemory:                   false,
		BinaryArchiveDir:            filepath.Join(os.TempDir(), "bin_consistency_archive"),
		StartNum:                    46147,
		PruneInterval:               *pruneInterval,
		MaxBlocks:                   *maxBlocks,
		ShardDepth:                  *shardDepth,
		ArchiveBucketSize:           *archiveBucketSize,
		ArchiveItemCacheLimit:       *archiveItemCacheLimit,
		CuckooBuckets:               *cuckooBuckets,
		CuckooSlots:                 *cuckooSlots,
		BinaryNodeCacheLimit:        *binaryNodeCacheLimit,
		BinaryNodeCacheBytesLimitMB: *binaryNodeCacheBytesLimitMB,
		BinaryPathDiagnostics:       *binaryPathDiagnostics,
		BinaryPruneShardMetrics:     *binaryPruneShardMetrics,
		BinaryAsyncPrune:            *binaryAsyncPrune,
		BinaryCommitWorkers:         *binaryCommitWorkers,
		BinaryCommitWatchdog:        *binaryCommitWatchdog,
		BinaryPhysicalDelete:        *binaryPhysicalDelete,
		BinaryNodeStorage:           *binaryNodeStorage,
	}
	os.RemoveAll(binCfg.DbDir)
	os.RemoveAll(binCfg.BinaryArchiveDir)
	defer os.RemoveAll(binCfg.DbDir)
	defer os.RemoveAll(binCfg.BinaryArchiveDir)
	binHost, err := NewProcessorHost(binCfg)
	if err != nil {
		t.Fatalf("failed to create Binary host: %v", err)
	}
	defer binHost.Close()

	files, err := compareFindTransactionFiles(binCfg.DataDir)
	if err != nil || len(files) == 0 {
		t.Fatalf("no transaction files found")
	}
	selectedFiles := files[binCfg.StartFileIdx-1 : binCfg.EndFileIdx]

	// Create genesis block and blockchain for both hosts
	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{},
	}
	gspec.MustCommit(mptHost.db, mptHost.trieDB)
	gspec.MustCommit(binHost.db, binHost.trieDB)

	mptLastRoot := types.EmptyRootHash
	binLastRoot := common.Hash{}

	mptTracer := newConsistencyTracer("MPT")
	binTracer := newConsistencyTracer("BIN")

	processedBlocks := 0

consistencyFiles:
	for _, file := range selectedFiles {
		ts, err := NewTransactionStreamer(file)
		if err != nil {
			t.Errorf("failed to open transaction streamer for %s: %v", file, err)
			continue
		}
		defer ts.Close()

		fileIdx := compareGetFileIndex(file)
		compareLoadBlockTimestampsFromFile(binCfg.DataDir, fileIdx)

		// Process blocks sequentially from the streamer
		currentBlock, ok := ts.PeekBlockNum()
		if !ok {
			continue // skip empty file
		}

		for {
			targetBlock, ok := ts.PeekBlockNum()
			if !ok {
				break
			}

			// Process empty blocks between transactions
			for currentBlock < targetBlock {
				if binCfg.MaxBlocks > 0 && processedBlocks >= binCfg.MaxBlocks {
					break consistencyFiles
				}
				b := currentBlock
				miner := compareBlockMiners[b]
				reward, _ := uint256.FromBig(new(big.Int).Mul(big.NewInt(1e6), big.NewInt(1e18)))

				// MPT
				mptTracer.Reset()
				mptTracer.blockNum = b
				mptHost.trieDB.UpdateBlockNum(b)
				mptStateDB, err := state.New(mptLastRoot, mptHost.sdb)
				if err != nil {
					t.Fatalf("failed to create MPT statedb at empty block %d: %v", b, err)
				}
				mptHooked := state.NewHookedState(mptStateDB, mptTracer.Hooks())
				if miner != (common.Address{}) {
					mptHooked.AddBalance(miner, reward, tracing.BalanceChangeUnspecified)
				}
				mptHooked.Finalise(false)
				mptRoot, _ := mptStateDB.Commit(b, false, false)

				// BIN
				binTracer.Reset()
				binTracer.blockNum = b
				binHost.trieDB.UpdateBlockNum(b)
				binStateDB, err := state.New(binLastRoot, binHost.sdb)
				if err != nil {
					t.Fatalf("failed to create BIN statedb at empty block %d: %v", b, err)
				}
				binHooked := state.NewHookedState(binStateDB, binTracer.Hooks())
				if miner != (common.Address{}) {
					binHooked.AddBalance(miner, reward, tracing.BalanceChangeUnspecified)
				}
				if binCfg.PruneInterval > 0 && b%uint64(binCfg.PruneInterval) == 0 {
					binStateDB.PruneNextShard()
				}
				binHooked.Finalise(false)
				binRoot, err := binStateDB.Commit(b, false, false)
				if err != nil {
					t.Fatalf("Block %d (empty): BIN Commit failed: %v", b, err)
				}
				mptSummary := mptTracer.Summary(b, mptRoot)
				binSummary := binTracer.Summary(b, binRoot)

				if mptSummary.WriteCount != binSummary.WriteCount {
					t.Fatalf("Block %d (empty): WriteCount mismatch! MPT=%d, BIN=%d", b, mptSummary.WriteCount, binSummary.WriteCount)
				}
				if mptSummary.WriteHash != binSummary.WriteHash {
					t.Fatalf("Block %d (empty): WriteHash mismatch! MPT=%s, BIN=%s, binRoot=%s", b, mptSummary.WriteHash.Hex(), binSummary.WriteHash.Hex(), binRoot.Hex())
				}

				mptLastRoot = mptRoot
				binLastRoot = binRoot
				if b%1000 == 0 {
					fmt.Printf("[Test] Processing empty block %d, binRoot=%s\n", b, binRoot.Hex())
					mptHost.trieDB.Commit(mptRoot, false)
					binHost.trieDB.Commit(binRoot, false)
				}
				if b > 0 && binLastRoot != (common.Hash{}) && binRoot == (common.Hash{}) {
					t.Fatalf("Block %d (empty): binRoot became zero! binLastRoot was %s", b, binLastRoot.Hex())
				}
				processedBlocks++
				currentBlock++
			}

			// Process the block with transactions
			if binCfg.MaxBlocks > 0 && processedBlocks >= binCfg.MaxBlocks {
				break consistencyFiles
			}
			b := targetBlock
			msgs, _ := ts.PopBlock(b)
			miner := compareBlockMiners[b]
			reward, _ := uint256.FromBig(new(big.Int).Mul(big.NewInt(1e6), big.NewInt(1e18)))

			// A. Process with MPT
			mptTracer.Reset()
			mptTracer.blockNum = b
			mptHost.trieDB.UpdateBlockNum(b) // Changed from mptHost.sdb.SetBlockNum(b)
			mptStateDB, err := state.New(mptLastRoot, mptHost.sdb)
			if err != nil {
				t.Fatalf("failed to create MPT statedb at block %d: %v", b, err)
			}
			// Pre-allocate balance for all senders
			bigBalance := new(big.Int).Mul(big.NewInt(1e15), big.NewInt(1e18))
			balance, _ := uint256.FromBig(bigBalance)
			for _, msg := range msgs {
				mptStateDB.SetBalance(msg.From, balance, tracing.BalanceChangeUnspecified)
			}

			mptHooked := state.NewHookedState(mptStateDB, mptTracer.Hooks())

			if miner != (common.Address{}) {
				mptHooked.AddBalance(miner, reward, tracing.BalanceChangeUnspecified)
			}
			blockCtx := vm.BlockContext{
				CanTransfer: core.CanTransfer,
				Transfer:    core.Transfer,
				GetHash:     func(n uint64) common.Hash { return common.Hash{} },
				Coinbase:    miner,
				BlockNumber: new(big.Int).SetUint64(targetBlock),
				Time:        compareBlockTimestamps[targetBlock],
				Difficulty:  big.NewInt(1),
				Random:      &common.Hash{},
				GasLimit:    1000000000,
				BaseFee:     big.NewInt(0),
				BlobBaseFee: big.NewInt(0),
			}

			mptEVM := vm.NewEVM(blockCtx, mptHooked, params.MainnetChainConfig, vm.Config{})
			for _, msg := range msgs {
				msg.SkipNonceChecks = true
				_, err := core.ApplyMessage(mptEVM, msg, new(core.GasPool).AddGas(msg.GasLimit))
				if err != nil && b%1000 == 0 {
					toStr := "contract-creation"
					if msg.To != nil {
						toStr = msg.To.Hex()
					}
					fmt.Printf("MPT Transaction Reverted: block=%d, sender=%s, nonce=%d, to=%s, res.Err=%v\n", b, msg.From.Hex(), msg.Nonce, toStr, err)
				}
			}
			mptHooked.Finalise(false)
			mptRoot, _ := mptStateDB.Commit(b, false, false)
			mptSummary := mptTracer.Summary(b, mptRoot)

			// B. Process with Binary Trie
			binTracer.Reset()
			binTracer.blockNum = b
			binSuccessCount := 0
			binHost.sdb.SetBlockNum(b)
			binStateDB, err := state.New(binLastRoot, binHost.sdb)
			if err != nil {
				t.Fatalf("failed to create BIN statedb at block %d: %v", b, err)
			}
			// Pre-allocate balance for all senders
			for _, msg := range msgs {
				binStateDB.SetBalance(msg.From, balance, tracing.BalanceChangeUnspecified)
			}

			binHooked := state.NewHookedState(binStateDB, binTracer.Hooks())

			if miner != (common.Address{}) {
				binHooked.AddBalance(miner, reward, tracing.BalanceChangeUnspecified)
			}
			binEVM := vm.NewEVM(blockCtx, binHooked, params.MainnetChainConfig, vm.Config{})

			for _, msg := range msgs {
				msg.SkipNonceChecks = true
				res, applyErr := core.ApplyMessage(binEVM, msg, new(core.GasPool).AddGas(msg.GasLimit))
				if applyErr != nil || (res != nil && res.Err != nil) {
					errMsg := applyErr
					if errMsg == nil {
						errMsg = res.Err
					}
					if b == 50107 {
						fmt.Printf("[BIN] Block 50107 Msg Fail: sender=%s, nonce=%d, err=%v\n", msg.From.Hex(), msg.Nonce, errMsg)
					}
					if b%1000 == 0 {
						toStr := "contract-creation"
						if msg.To != nil {
							toStr = msg.To.Hex()
						}
						fmt.Printf("BIN Transaction Reverted: block=%d, sender=%s, nonce=%d, to=%s, res.Err=%v\n", b, msg.From.Hex(), msg.Nonce, toStr, errMsg)
					}
				} else {
					binSuccessCount++
				}
			}

			if binCfg.PruneInterval > 0 && b%uint64(binCfg.PruneInterval) == 0 {
				binStateDB.PruneNextShard()
			}

			binHooked.Finalise(false)
			binRoot, err := binStateDB.Commit(b, false, false)
			if err != nil {
				t.Fatalf("Block %d: BIN Commit failed: %v", b, err)
			}
			binSummary := binTracer.Summary(b, binRoot)

			mptLastRoot = mptRoot
			binLastRoot = binRoot

			if b%1000 == 0 {
				mptHost.trieDB.Commit(mptRoot, false)
				binHost.trieDB.Commit(binRoot, false)
			}

			// C. Compare
			if mptSummary.WriteCount != binSummary.WriteCount {
				t.Fatalf("Block %d: WriteCount mismatch! MPT=%d, BIN=%d", b, mptSummary.WriteCount, binSummary.WriteCount)
			}
			if mptSummary.WriteHash != binSummary.WriteHash {
				t.Fatalf("Block %d: WriteHash mismatch! MPT=%s, BIN=%s (WriteCount=%d), binSuccess=%d, binRoot=%s", b, mptSummary.WriteHash.Hex(), binSummary.WriteHash.Hex(), mptSummary.WriteCount, binSuccessCount, binRoot.Hex())
			}

			if b == 52313 {
				targetAddr := common.HexToAddress("0x59622442B567187157b85d6928A6c56e1E0841CA")
				targetSlot := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
				binVal := binStateDB.GetState(targetAddr, targetSlot)
				mptVal := mptStateDB.GetState(targetAddr, targetSlot)
				fmt.Printf("DEBUG: Block 52313: BIN GetState(0x5962..CA, slot02) = %s\n", binVal.Hex())
				fmt.Printf("DEBUG: Block 52313: MPT GetState(0x5962..CA, slot02) = %s\n", mptVal.Hex())
			}
			// Note: Roots will be different because MPT and Binary Trie have different structures.
			// But the state changes (captured by tracer) must be identical.

			mptLastRoot = mptRoot
			binLastRoot = binRoot

			if b%1000 == 0 {
				mptHost.trieDB.Commit(mptRoot, false)
				binHost.trieDB.Commit(binRoot, false)
				fmt.Printf("[Test] Consistency check passed up to block %d\n", b)
			}
			processedBlocks++
			currentBlock++
		}
	}
}
