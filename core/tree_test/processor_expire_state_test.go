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
	dbDir                 = flag.String("dbDir2", "F:\\expire_data\\expire_state_db", "Database directory")
	dataDir               = flag.String("dataDir2", "E:\\ethdata", "Input data directory")
	startIdx              = flag.Int("startFileIdx2", 1, "Start file index")
	endIdx                = flag.Int("endFileIdx2", 21, "End file index")
	useVerkle             = flag.Bool("useVerkle2", false, "Enable Verkle trie")
	useBinaryTrie         = flag.Bool("useBinaryTrie2", true, "Enable Binary trie")
	useKV                 = flag.Bool("useKV2", false, "Enable no-hash KV state backend")
	useMemory             = flag.Bool("useMemory2", false, "Use in-memory DB")
	binaryArchiveDir      = flag.String("binaryArchiveDir2", "F:\\expire_data\\expire_state_db_achive", "Binary trie archive directory")
	metricsDir            = flag.String("metricsDir2", ".", "Directory for mainnet metrics CSV/JSON output")
	statsInterval         = flag.Int("statsInterval2", 100000, "Statistics reporting interval (in blocks)")
	fullTrieStatsInterval = flag.Int("fullTrieStatsInterval", 2000000, "Blocks between exact full-trie structure scans; 0 only scans at the end")
	pruneInterval         = flag.Int("pruneInterval", 1, "Blocks between Trie.PruneNextShard() calls")
	maxBlocks             = flag.Int("blocks", 0, "Maximum number of blocks to process during processor or consistency tests (0 = all)")

	// Binary Trie Ablation flags
	shardDepth                  = flag.Int("shardDepth", 8, "Binary trie shard depth")
	archiveBucketSize           = flag.Int("archiveBucketSize", 100, "Binary trie archive bucket size")
	cuckooBuckets               = flag.Int("cuckooBuckets", 16, "Binary trie cuckoo filter buckets")
	cuckooSlots                 = flag.Int("cuckooSlots", 4, "Binary trie cuckoo filter slots")
	binaryNodeCacheLimit        = flag.Int("binaryNodeCacheLimit", archivetrie.DefaultNodeCacheLimit, "Binary trie process node cache entry limit; 0 uses default, negative disables cache")
	binaryNodeCacheBytesLimitMB = flag.Int("binaryNodeCacheBytesLimitMB", 512, "Binary trie process node cache byte limit in MiB; 0 uses default, negative disables byte cap")
	binaryPathDiagnostics       = flag.Bool("binaryPathDiagnostics", false, "Enable binary trie path/cache diagnostics")
	binaryPruneShardMetrics     = flag.Bool("binaryPruneShardMetrics", false, "Write per-prune binary shard pressure metrics CSV")
	binaryAsyncPrune            = flag.Bool("binaryAsyncPrune", false, "Run binary shard pruning asynchronously and wait before root commit")
	binaryCommitWorkers         = flag.Int("binaryCommitWorkers", 0, "Max parallel binary shard commit workers; 0 uses binary default cap")
	binaryCommitWatchdog        = flag.Int("binaryCommitWatchdogSec", 0, "Dump goroutines if one binary wrapper commit exceeds this many seconds; 0 disables")
	binaryPhysicalDelete        = flag.Bool("binaryPhysicalDelete", false, "Physically delete obsolete binary trie state nodes from stateDB")
	binaryStemArchive           = flag.Bool("binaryStemArchive", false, "Group binary-tree state by 31-byte stem and archive all 256 suffixes together")
	binaryNodeStorage           = flag.String("binaryNodeStorage", "path", "Binary trie node storage scheme: hash or path")
	archiveOverlapBudgetMs      = flag.Int("archiveOverlapBudgetMs", 8000, "Async archive wait budget that can overlap block interval and is not charged to root compute")
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
	DbDir                 string
	DataDir               string
	StartFileIdx          int
	EndFileIdx            int
	UseVerkle             bool
	UseBinaryTrie         bool
	UseKV                 bool
	UseMemory             bool
	BinaryArchiveDir      string
	MetricsDir            string
	StartNum              uint64
	PruneInterval         int
	MaxBlocks             int
	FullTrieStatsInterval int

	// Ablation params
	ShardDepth                  int
	ArchiveBucketSize           int
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
	BinaryStemArchive           bool
	BinaryNodeStorage           string
	ArchiveOverlapBudgetMs      int
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
			CuckooBuckets:         cfg.CuckooBuckets,
			CuckooSlots:           cfg.CuckooSlots,
			NodeCacheLimit:        cfg.BinaryNodeCacheLimit,
			NodeCacheBytesLimit:   int64(cfg.BinaryNodeCacheBytesLimitMB) * 1024 * 1024,
			EnablePathDiagnostics: cfg.BinaryPathDiagnostics,
			AsyncPrune:            cfg.BinaryAsyncPrune,
			CommitWorkers:         cfg.BinaryCommitWorkers,
			CommitWatchdogSeconds: cfg.BinaryCommitWatchdog,
			PhysicalDelete:        cfg.BinaryPhysicalDelete,
			StemMode:              cfg.BinaryStemArchive,
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
		FullTrieStatsInterval:       *fullTrieStatsInterval,
		ShardDepth:                  *shardDepth,
		ArchiveBucketSize:           *archiveBucketSize,
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
		BinaryStemArchive:           *binaryStemArchive,
		BinaryNodeStorage:           *binaryNodeStorage,
		ArchiveOverlapBudgetMs:      *archiveOverlapBudgetMs,
		MaxRootPipelineMs:           *maxRootPipelineMs,
		MaxHandleDestructMs:         *maxHandleDestructMs,
		MaxPruningMs:                *maxPruningMs,
	}
	fmt.Printf("[ASCT_CONFIG] stemArchive=%t shardDepth=%d bucketSize=%d nodeStorage=%s pruneInterval=%d\n",
		cfg.BinaryStemArchive, cfg.ShardDepth, cfg.ArchiveBucketSize, cfg.BinaryNodeStorage, cfg.PruneInterval)

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
		intervalBlocks               uint64
		totalProcessedBlocks         uint64
		epochID                      uint64
		totalTxTime                  time.Duration
		maxTxTime                    time.Duration
		maxTxTimeBlock               uint64
		totalFinaliseTime            time.Duration
		maxFinaliseTime              time.Duration
		totalCommitTime              time.Duration
		maxCommitTime                time.Duration
		maxCommitBlock               uint64
		totalStatePreCommit          time.Duration
		maxStatePreCommit            time.Duration
		maxStatePreCommitBlock       uint64
		totalIntermediateFinalise    time.Duration
		maxIntermediateFinalise      time.Duration
		maxIntermediateFinaliseBlock uint64
		totalStorageUpdates          time.Duration
		maxStorageUpdates            time.Duration
		maxStorageUpdatesBlock       uint64
		totalStorageUpdateWork       time.Duration
		maxStorageUpdateObject       time.Duration
		maxStorageUpdateObjectBlock  uint64
		maxStorageUpdateObjectSlots  int64
		totalStorageUpdateObjects    int64
		totalStorageUpdateSlots      int64
		maxStorageUpdateWorkers      int64
		totalAccountUpdates          time.Duration
		maxAccountUpdates            time.Duration
		maxAccountUpdatesBlock       uint64
		totalAccountHashes           time.Duration
		maxAccountHashes             time.Duration
		maxAccountHashesBlock        uint64
		totalIntermediateMutations   int64
		totalAccountUpdated          int64
		totalAccountDeleted          int64
		totalStorageUpdated          int64
		totalStorageDeleted          int64
		totalPostCommit              time.Duration
		maxPostCommit                time.Duration
		maxPostCommitBlock           uint64
		totalRootPipelineTime        time.Duration
		maxRootPipelineTime          time.Duration
		maxRootPipelineBlock         uint64
		totalRootComputeTime         time.Duration
		maxRootComputeTime           time.Duration
		maxRootComputeBlock          uint64
		totalRootDBWriteTime         time.Duration
		maxRootDBWriteTime           time.Duration
		maxRootDBWriteBlock          uint64
		totalHandleDestruct          time.Duration
		maxHandleDestruct            time.Duration
		maxHandleDestructBlock       uint64
		totalAccountStateWipe        time.Duration
		maxAccountStateWipe          time.Duration
		maxAccountWipeBlock          uint64
		totalDestroyedAccounts       uint64
		totalWipedAccounts           uint64
		totalWipedStorageSlots       uint64
		totalWipedCodeChunks         uint64
		totalWipedStemRecords        uint64
		totalWipeIndexScan           time.Duration
		totalWipeStemDelete          time.Duration
		totalWipeIndexStage          time.Duration
		totalWipeOriginBuild         time.Duration
		totalHashShardWall           time.Duration
		totalHashTotal               time.Duration
		totalHashShardWork           time.Duration
		totalHashSerialize           time.Duration
		totalHashCompute             time.Duration
		totalHashRootMerge           time.Duration
		maxHashTotal                 time.Duration
		maxHashBlock                 uint64
		totalHashDirtyShards         int64
		totalHashWorkers             int64
		totalHashNodes               int64
		totalHashDirtyNodes          int64
		totalHashCleanNodes          int64
		totalPruneTime               time.Duration
		maxPruneTime                 time.Duration
		maxPruneBlock                uint64
		totalArchiveCompute          time.Duration
		maxArchiveCompute            time.Duration
		totalArchiveWait             time.Duration
		maxArchiveWait               time.Duration
		totalArchiveWaitExcess       time.Duration
		maxArchiveWaitExcess         time.Duration
		maxArchiveWaitBlock          uint64
		maxProofSizeBlockBlock       uint64
		pruneCount                   uint64
		totalStorageSize             int64 // Cumulative storage size
		intervalTxCount              uint64
		intervalSuccessTxCount       uint64
		globalTxCount                uint64
		globalSuccessTxCount         uint64
		lastNodeCacheHits            int64
		lastNodeCacheMisses          int64
		lastNodeCacheEvictions       int64
		lastNodeCacheLockContentions int64
		lastNodeCacheLockWaitNanos   int64
		lastNodeCacheDBGets          int64
		lastNodeCacheDBGetNanos      int64
		lastNodeCacheDBLoadBytes     int64
		lastUpdateDiagnostics        archivetrie.UpdateDiagnostics
	)
	recordPreCommitBreakdown := func(block uint64, statedb *state.StateDB) {
		totalIntermediateFinalise += statedb.IntermediateFinalise
		if statedb.IntermediateFinalise > maxIntermediateFinalise {
			maxIntermediateFinalise = statedb.IntermediateFinalise
			maxIntermediateFinaliseBlock = block
		}
		totalStorageUpdates += statedb.StorageUpdates
		if statedb.StorageUpdates > maxStorageUpdates {
			maxStorageUpdates = statedb.StorageUpdates
			maxStorageUpdatesBlock = block
		}
		totalStorageUpdateWork += statedb.StorageUpdateWork
		if statedb.StorageUpdateMaxObject > maxStorageUpdateObject {
			maxStorageUpdateObject = statedb.StorageUpdateMaxObject
			maxStorageUpdateObjectBlock = block
			maxStorageUpdateObjectSlots = statedb.StorageUpdateMaxObjectSlots
		}
		totalStorageUpdateObjects += statedb.StorageUpdateObjects
		totalStorageUpdateSlots += statedb.StorageUpdateSlots
		maxStorageUpdateWorkers = max(maxStorageUpdateWorkers, statedb.StorageUpdateWorkers)
		totalAccountUpdates += statedb.AccountUpdates
		if statedb.AccountUpdates > maxAccountUpdates {
			maxAccountUpdates = statedb.AccountUpdates
			maxAccountUpdatesBlock = block
		}
		totalAccountHashes += statedb.AccountHashes
		if statedb.AccountHashes > maxAccountHashes {
			maxAccountHashes = statedb.AccountHashes
			maxAccountHashesBlock = block
		}
		totalIntermediateMutations += statedb.IntermediateMutations
		totalAccountUpdated += int64(statedb.AccountUpdated)
		totalAccountDeleted += int64(statedb.AccountDeleted)
		totalStorageUpdated += statedb.StorageUpdated.Load()
		totalStorageDeleted += statedb.StorageDeleted.Load()
	}
	recordHashBlockStats := func(block uint64, diag archivetrie.HashDiagnostics) {
		if !cfg.UseBinaryTrie {
			return
		}
		totalHashTotal += time.Duration(diag.TotalNanos)
		totalHashShardWall += time.Duration(diag.ShardWallNanos)
		totalHashShardWork += time.Duration(diag.ShardWorkNanos)
		totalHashSerialize += time.Duration(diag.SerializeNanos)
		totalHashCompute += time.Duration(diag.HashNanos)
		totalHashRootMerge += time.Duration(diag.RootNanos)
		totalHashDirtyShards += diag.DirtyShards
		totalHashWorkers += diag.Workers
		totalHashNodes += diag.NodeCount
		totalHashDirtyNodes += diag.DirtyNodes
		totalHashCleanNodes += diag.CleanNodes
		if total := time.Duration(diag.TotalNanos); total > maxHashTotal {
			maxHashTotal = total
			maxHashBlock = block
		}
	}
	slowCommitDiagThreshold := 400 * time.Millisecond
	archiveOverlapBudget := time.Duration(cfg.ArchiveOverlapBudgetMs) * time.Millisecond
	if archiveOverlapBudget < 0 {
		archiveOverlapBudget = 0
	}

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
			"Lock_Wait_us", "Root_Load_us", "Walk_us", "Finish_us", "Detailed_Counters_Enabled",
			"Internal_Visits", "Hot_Skips", "Child_Hits", "Child_Skips", "Bulk_Collects",
			"Collected_Leaves", "Collected_Stubs", "Build_Items", "Build_Buckets", "ArchiveBuild_Parallel",
			"Path_Absorbed_Items", "Root_Pool_Items", "Root_Pool_Buckets",
		})
	}
	if cfg.UseBinaryTrie {
		archivetrie.ResetPrunePressureDiagnostics()
		archivetrie.ResetArchiveCumulativeDiagnostics()
	}
	var lastFullTrieStats *archivetrie.TrieStats
	var lastFullTrieStatsBlock uint64

	// Write CSV Header
	metricsHeader := []string{
		"Epoch_ID", "Tree_Type", "Cumulative_Storage_Bytes",
		"State_Storage_Bytes", "Archived_Storage_Bytes",
		"State_Storage_Share_Pct", "Archive_Storage_Share_Pct",
		// 内存相关列用于判断是否真泄漏：Heap 持续增长说明仍有长期引用；
		// NodeCache_MB 顶到上限则说明缓存保护生效，后续可调 bytes limit 做性能/内存折中。
		"Heap_Alloc_MB", "Heap_Sys_MB", "Runtime_Sys_MB", "NodeCache_MB", "NodeCache_Entries",
		"Stem_Archive_Mode",
		"Archive_Bytes_Per_Item", "State_Bytes_Per_Active_Leaf",
		"Archive_Bytes_Per_Logical_Value", "State_Bytes_Per_Active_Logical_Value",
		"Trie_Child_Node_Count", "Total_Archived_Items", "Total_Bucket_Count",
		"Active_Logical_Values", "Archived_Logical_Values",
		"Active_Logical_Value_Read_Failures", "Archived_Logical_Value_Read_Failures",
		"Root_Bucket_Count", "Root_Archived_Items", "Root_Leaf_Bucket_Count", "Root_Leaf_Archived_Items",
		"Stub_Bucket_Count", "Stub_Archived_Items", "Child_Bucket_Count", "Child_Archived_Items",
		"Root_Stub_Bucket_Count", "Root_Stub_Archived_Items", "Deep_Stub_Bucket_Count", "Deep_Stub_Archived_Items",
		"Max_StubList_Buckets", "Max_StubList_Items", "Max_Root_StubList_Buckets", "Max_Root_StubList_Items",
		"Max_Deep_StubList_Buckets", "Max_Deep_StubList_Items",
		"Max_Buckets_On_Single_Path", "Bucket_Items_Avg", "Bucket_Items_P50", "Bucket_Items_P95", "Bucket_Items_P99", "Bucket_Items_Max",
		"Avg_Finalise_Time_ms", "Max_Finalise_Time_ms",
		"Avg_State_Commit_Time_ms", "Max_State_Commit_Time_ms",
		"Avg_State_PreCommit_Time_ms", "Max_State_PreCommit_Time_ms",
		"Avg_State_PostCommit_Time_ms", "Max_State_PostCommit_Time_ms",
		"Avg_Root_Pipeline_Wall_Time_ms", "Max_Root_Pipeline_Wall_Time_ms",
		"Avg_Root_Compute_Charged_Time_ms", "Max_Root_Compute_Charged_Time_ms",
		"Avg_Root_DB_Write_Time_ms", "Max_Root_DB_Write_Time_ms",
		"Avg_Archive_Wait_Over_Budget_ms", "Max_Archive_Wait_Over_Budget_ms", "Archive_Overlap_Budget_ms",
		"Max_State_Commit_Block", "Max_State_PreCommit_Block", "Max_State_PostCommit_Block",
		"Max_Root_Pipeline_Block", "Max_Root_Compute_Block",
		"Max_Root_DB_Write_Block", "Max_Archive_Wait_Over_Budget_Block",
		"Avg_Handle_Destruction_Time_ms", "Max_Handle_Destruction_Time_ms", "Max_Handle_Destruction_Block",
		"Avg_Account_State_Wipe_Time_ms", "Max_Account_State_Wipe_Time_ms", "Max_Account_State_Wipe_Block",
		"Destroyed_Accounts", "Indexed_Wiped_Accounts", "Indexed_Wiped_Storage_Slots", "Indexed_Wiped_Code_Chunks",
		"Avg_Pruning_Time_us", "Max_Pruning_Time_us",
		"Avg_Archive_Compute_Time_us", "Max_Archive_Compute_Time_us",
		"Avg_Archive_Wait_Time_us", "Max_Archive_Wait_Time_us",
		"Max_Pruning_Shard_ID", "Max_Pruning_Shard_Time_us", "Max_Pruning_Shard_Total_us",
		"Max_Pruning_Shard_Wait_us", "Max_Pruning_Shard_Prefetch_us",
		"Max_Pruning_Shard_Lock_Wait_us", "Max_Pruning_Shard_Root_Load_us",
		"Max_Pruning_Shard_Walk_us", "Max_Pruning_Shard_Finish_us", "Pruning_Detailed_Counters_Enabled",
		"Max_Pruning_Shard_Internal_Visits", "Max_Pruning_Shard_Hot_Skips",
		"Max_Pruning_Shard_Child_Hits", "Max_Pruning_Shard_Child_Skips",
		"Max_Pruning_Shard_Bulk_Collects", "Max_Pruning_Shard_Collected_Leaves",
		"Max_Pruning_Shard_Collected_Stubs", "Max_Pruning_Shard_Build_Items",
		"Max_Pruning_Shard_Build_Buckets", "Max_Pruning_Shard_ArchiveBuild_Parallel",
		"Max_Pruning_Shard_Path_Absorbed_Items", "Max_Pruning_Shard_Root_Pool_Items",
		"Max_Pruning_Shard_Root_Pool_Buckets",
		"Hit_Count", "Miss_NonExistent_Count", "Miss_Existent_Count",
		"Avg_Proof_Gen_Time_ms", "Max_Proof_Gen_Time_ms", "Avg_Proof_Verify_Time_ms", "Max_Proof_Verify_Time_ms",
		"Avg_Proof_Size_Byte", "Max_Proof_Size_Byte",
		"Max_Pruning_Block", "Max_Proof_Size_Block",
		"Block_Start", "Block_End",
		"Item_Proof_Min", "Item_Proof_P25", "Item_Proof_Med", "Item_Proof_P75", "Item_Proof_Max",
		"Cycle_FP_Count", "Max_FP_In_Single_Block",
		"Cumulative_Archived_Leaves", "Cumulative_Flat_Value_Puts", "Cumulative_Flat_Value_Deletes",
		"Cumulative_Flat_Value_Put_Bytes", "Cumulative_Archive_Event_Rate_Pct", "Cumulative_Archived_vs_Active_Pct",
		"Storage_Layout", "Shared_DB_Bytes", "Archive_Storage_Bytes_Valid",
		"Avg_Tx_Execution_ms", "Max_Tx_Execution_ms", "Max_Tx_Execution_Block",
		"Interval_Tx_Count", "Interval_Success_Tx_Count", "Tx_Execution_TPS",
		"Metrics_Collection_ms", "State_Dir_Scan_ms", "Archive_Dir_Scan_ms", "Trie_Stats_ms", "Trie_Stats_Exact", "Trie_Stats_Block",
		"NodeCache_Total_Hits", "NodeCache_Total_Misses", "NodeCache_Total_Evictions",
		"NodeCache_Window_Hits", "NodeCache_Window_Misses", "NodeCache_Window_Evictions",
		"Avg_Hash_Total_ms", "Avg_Hash_Shard_Wall_ms", "Avg_Hash_Shard_Work_ms", "Avg_Hash_Serialize_ms",
		"Avg_Hash_Compute_ms", "Avg_Hash_Root_Merge_ms", "Max_Hash_Total_ms", "Max_Hash_Block",
		"Avg_Hash_Dirty_Shards", "Avg_Hash_Workers", "Avg_Hash_Nodes", "Avg_Hash_Dirty_Nodes", "Avg_Hash_Clean_Nodes",
		"Indexed_Wiped_Stem_Records", "Wipe_Index_Scan_ms", "Wipe_Stem_Delete_ms",
		"Wipe_Index_Stage_ms", "Wipe_Origin_Build_ms",
		"Avg_Intermediate_Finalise_ms", "Max_Intermediate_Finalise_ms", "Max_Intermediate_Finalise_Block",
		"Avg_Storage_Updates_ms", "Max_Storage_Updates_ms", "Max_Storage_Updates_Block",
		"Avg_Storage_Update_Work_ms", "Max_Storage_Object_ms", "Max_Storage_Object_Block", "Max_Storage_Object_Slots",
		"Avg_Storage_Objects", "Avg_Storage_Slots", "Max_Storage_Workers",
		"Avg_Account_Updates_ms", "Max_Account_Updates_ms", "Max_Account_Updates_Block",
		"Avg_Account_Hashes_ms", "Max_Account_Hashes_ms", "Max_Account_Hashes_Block",
		"Intermediate_Mutations", "Account_Updated", "Account_Deleted", "Storage_Updated", "Storage_Deleted",
		"Stem_Put_Calls", "Stem_Put_Noops", "Stem_Put_Loaded_Bytes", "Stem_Put_Encoded_Bytes", "Stem_Put_Commitment_Hashes",
		"Stem_Put_Total_ms", "Stem_Put_Load_ms", "Stem_Put_Decode_ms", "Stem_Put_Encode_ms", "Stem_Put_Backend_ms",
		"Stem_Apply_Calls", "Stem_Apply_Updates", "Stem_Apply_Stems", "Stem_Apply_Puts", "Stem_Apply_Deletes",
		"Stem_Apply_Total_ms", "Stem_Apply_Load_ms", "Stem_Apply_Encode_ms", "Stem_Apply_Backend_ms",
		"Shard_Put_Calls", "Shard_Put_Values", "Shard_Put_ms",
		"Shard_PutBatch_Calls", "Shard_PutBatch_Values", "Shard_PutBatch_Shards", "Shard_PutBatch_Wall_ms", "Shard_PutBatch_Work_ms",
		"Shard_Delete_Calls", "Shard_Delete_ms",
		"Flat_Value_Gets", "Flat_Value_Read_IO_ms", "Flat_Value_Read_IO_Bytes",
		"Flat_Value_Puts", "Flat_Value_Deletes", "Flat_Value_Write_ms",
		"Archive_Promotion_Checks", "Archive_Promotion_Hits",
		"NodeCache_Entry_Limit", "NodeCache_Bytes_Limit", "NodeCache_Shards",
		"NodeCache_Window_Lock_Contentions", "NodeCache_Window_Lock_Wait_ms",
		"NodeCache_Window_DB_Gets", "NodeCache_Window_DB_Get_ms", "NodeCache_Window_DB_Load_Bytes",
	}
	writer.Write(metricsHeader)
	kvStatsWriter.Write([]string{
		"Block",
		"Reads", "Read_3M", "Read_6M", "Read_1Y", "Read_NonExistent",
		"Writes", "Write_3M", "Write_6M", "Write_1Y", "Write_NonExistent",
	})

	reportStats := func(final bool) {
		if intervalBlocks == 0 {
			return
		}
		metricsStart := time.Now()
		epochID++
		treeType := "MPT"
		if cfg.UseBinaryTrie {
			treeType = "ASCT"
		} else if cfg.UseVerkle {
			treeType = "Verkle"
		} else if cfg.UseKV {
			treeType = "KV"
		}
		stateDirScanStart := time.Now()
		stateStorageSize, _ := getDirSize(cfg.DbDir)
		stateDirScanDuration := time.Since(stateDirScanStart)
		archiveStorageSize := int64(0)
		archiveDirScanDuration := time.Duration(0)
		storageLayout := "separate_databases"
		archiveStorageBytesValid := true
		if cfg.UseBinaryTrie {
			// ASCT currently persists both hot and archived records in DbDir. The
			// configured archive database is opened but not used by ArchiveTrie.
			storageLayout = "shared_state_db"
			archiveStorageBytesValid = false
			archiveStorageSize = -1
		} else if cfg.BinaryArchiveDir != "" {
			archiveDirScanStart := time.Now()
			archiveStorageSize, _ = getDirSize(cfg.BinaryArchiveDir)
			archiveDirScanDuration = time.Since(archiveDirScanStart)
		}
		totalStorageSize = stateStorageSize
		if archiveStorageBytesValid {
			totalStorageSize += archiveStorageSize
		}

		var (
			trieChildNodeCount     int64
			totalArchivedItems     int64
			activeLogicalValues    int64
			archivedLogicalValues  int64
			activeLogicalFailures  int64
			archiveLogicalFailures int64
			totalBucketCount       int
			rootBucketCount        int
			rootArchivedItems      int64
			rootLeafBucketCount    int
			rootLeafItems          int64
			stubBucketCount        int
			stubArchivedItems      int64
			rootStubBucketCount    int
			rootStubItems          int64
			deepStubBucketCount    int
			deepStubItems          int64
			childBucketCount       int
			childArchivedItems     int64
			maxStubListBuckets     int
			maxStubListItems       int64
			maxRootStubBuckets     int
			maxRootStubItems       int64
			maxDeepStubBuckets     int
			maxDeepStubItems       int64
			maxBucketsPath         int
			bucketItemsAvg         float64
			bucketItemsP50         int
			bucketItemsP95         int
			bucketItemsP99         int
			bucketItemsMax         int
		)
		trieStatsDuration := time.Duration(0)
		trieStatsExact := false
		if cfg.UseBinaryTrie {
			if active := host.trieDB.GetArchiveTrie(); active != nil {
				if bt, ok := active.(*archivetrie.Trie); ok {
					runFullStats := final
					if cfg.FullTrieStatsInterval > 0 && totalProcessedBlocks%uint64(cfg.FullTrieStatsInterval) == 0 {
						runFullStats = true
					}
					if runFullStats {
						trieStatsStart := time.Now()
						lastFullTrieStats = bt.Stats()
						trieStatsDuration = time.Since(trieStatsStart)
						lastFullTrieStatsBlock = totalProcessedBlocks
						trieStatsExact = true
					}
					stats := lastFullTrieStats
					if stats != nil {
						trieChildNodeCount = stats.LeafCount
						totalArchivedItems = stats.ArchivedDataSize
						activeLogicalValues = stats.ActiveLogicalValues
						archivedLogicalValues = stats.ArchivedLogicalValues
						activeLogicalFailures = stats.ActiveLogicalValueReadFailures
						archiveLogicalFailures = stats.ArchivedLogicalValueReadFailures
						totalBucketCount = stats.BucketCount
						rootBucketCount = stats.RootBucketCount
						rootArchivedItems = stats.RootArchivedSize
						rootLeafBucketCount = stats.RootLeafBucketCount
						rootLeafItems = stats.RootLeafArchivedSize
						stubBucketCount = stats.StubBucketCount
						stubArchivedItems = stats.StubArchivedSize
						rootStubBucketCount = stats.RootStubBucketCount
						rootStubItems = stats.RootStubArchivedSize
						deepStubBucketCount = stats.DeepStubBucketCount
						deepStubItems = stats.DeepStubArchivedSize
						childBucketCount = stats.ChildBucketCount
						childArchivedItems = stats.ChildArchivedSize
						maxStubListBuckets = stats.MaxStubListBuckets
						maxStubListItems = stats.MaxStubListItems
						maxRootStubBuckets = stats.MaxRootStubBuckets
						maxRootStubItems = stats.MaxRootStubItems
						maxDeepStubBuckets = stats.MaxDeepStubBuckets
						maxDeepStubItems = stats.MaxDeepStubItems
						maxBucketsPath = stats.MaxBucketsPath
						bucketItemsAvg = stats.BucketItemsAvg
						bucketItemsP50 = stats.BucketItemsP50
						bucketItemsP95 = stats.BucketItemsP95
						bucketItemsP99 = stats.BucketItemsP99
						bucketItemsMax = stats.BucketItemsMax
					}
				}
			}
		}
		stateStorageSharePct := -1.0
		archiveStorageSharePct := -1.0
		if totalStorageSize > 0 && archiveStorageBytesValid {
			stateStorageSharePct = float64(stateStorageSize) * 100 / float64(totalStorageSize)
			archiveStorageSharePct = float64(archiveStorageSize) * 100 / float64(totalStorageSize)
		}
		archiveBytesPerItem := -1.0
		if totalArchivedItems > 0 && archiveStorageBytesValid {
			archiveBytesPerItem = float64(archiveStorageSize) / float64(totalArchivedItems)
		}
		stateBytesPerActiveLeaf := -1.0
		if trieChildNodeCount > 0 && archiveStorageBytesValid {
			stateBytesPerActiveLeaf = float64(stateStorageSize) / float64(trieChildNodeCount)
		}
		archiveBytesPerLogicalValue := -1.0
		if archivedLogicalValues > 0 && archiveStorageBytesValid {
			archiveBytesPerLogicalValue = float64(archiveStorageSize) / float64(archivedLogicalValues)
		}
		stateBytesPerActiveLogicalValue := -1.0
		if activeLogicalValues > 0 && archiveStorageBytesValid {
			stateBytesPerActiveLogicalValue = float64(stateStorageSize) / float64(activeLogicalValues)
		}
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		var commitDiag archivetrie.CommitDiagnostics
		if cfg.UseBinaryTrie {
			// LastCommitDiagnostics 里带有 wrapper 每次 commit 记录的 node cache 体量。
			commitDiag = archivetrie.LastCommitDiagnostics()
		}
		cacheWindowHits := commitDiag.NodeCacheTotalHits - lastNodeCacheHits
		cacheWindowMisses := commitDiag.NodeCacheTotalMisses - lastNodeCacheMisses
		cacheWindowEvictions := commitDiag.NodeCacheEvictions - lastNodeCacheEvictions
		cacheWindowLockContentions := commitDiag.NodeCacheLockContentions - lastNodeCacheLockContentions
		cacheWindowLockWaitNanos := commitDiag.NodeCacheLockWaitNanos - lastNodeCacheLockWaitNanos
		cacheWindowDBGets := commitDiag.NodeCacheDBGets - lastNodeCacheDBGets
		cacheWindowDBGetNanos := commitDiag.NodeCacheDBGetNanos - lastNodeCacheDBGetNanos
		cacheWindowDBLoadBytes := commitDiag.NodeCacheDBLoadBytes - lastNodeCacheDBLoadBytes
		if cacheWindowHits < 0 {
			cacheWindowHits = commitDiag.NodeCacheTotalHits
		}
		if cacheWindowMisses < 0 {
			cacheWindowMisses = commitDiag.NodeCacheTotalMisses
		}
		if cacheWindowEvictions < 0 {
			cacheWindowEvictions = commitDiag.NodeCacheEvictions
		}
		if cacheWindowLockContentions < 0 {
			cacheWindowLockContentions = commitDiag.NodeCacheLockContentions
		}
		if cacheWindowLockWaitNanos < 0 {
			cacheWindowLockWaitNanos = commitDiag.NodeCacheLockWaitNanos
		}
		if cacheWindowDBGets < 0 {
			cacheWindowDBGets = commitDiag.NodeCacheDBGets
		}
		if cacheWindowDBGetNanos < 0 {
			cacheWindowDBGetNanos = commitDiag.NodeCacheDBGetNanos
		}
		if cacheWindowDBLoadBytes < 0 {
			cacheWindowDBLoadBytes = commitDiag.NodeCacheDBLoadBytes
		}
		lastNodeCacheHits = commitDiag.NodeCacheTotalHits
		lastNodeCacheMisses = commitDiag.NodeCacheTotalMisses
		lastNodeCacheEvictions = commitDiag.NodeCacheEvictions
		lastNodeCacheLockContentions = commitDiag.NodeCacheLockContentions
		lastNodeCacheLockWaitNanos = commitDiag.NodeCacheLockWaitNanos
		lastNodeCacheDBGets = commitDiag.NodeCacheDBGets
		lastNodeCacheDBGetNanos = commitDiag.NodeCacheDBGetNanos
		lastNodeCacheDBLoadBytes = commitDiag.NodeCacheDBLoadBytes
		updateDiagnosticsNow := archivetrie.LastUpdateDiagnostics()
		windowUpdateDiagnostics := updateDiagnosticsNow.Sub(lastUpdateDiagnostics)
		lastUpdateDiagnostics = updateDiagnosticsNow
		txExecutionTPS := 0.0
		if totalTxTime > 0 {
			txExecutionTPS = float64(intervalSuccessTxCount) / totalTxTime.Seconds()
		}

		fmt.Printf("  Blocks: %d - %d (Processed Blocks Count)\n", totalProcessedBlocks-intervalBlocks, totalProcessedBlocks-1)
		fmt.Printf("  Tx Execution   - Avg: %v, Max: %v\n", totalTxTime/time.Duration(intervalBlocks), maxTxTime)
		if intervalTxCount > 0 {
			fmt.Printf("  Tx Success Rate - %.2f%% (%d/%d)\n", float64(intervalSuccessTxCount)*100/float64(intervalTxCount), intervalSuccessTxCount, intervalTxCount)
		}
		fmt.Printf("  Finalise - Avg: %.2f ms, Max: %v\n", float64(totalFinaliseTime.Milliseconds())/float64(intervalBlocks), maxFinaliseTime)
		fmt.Printf("  State Commit - Avg: %.2f ms, Max: %v\n", float64(totalCommitTime.Milliseconds())/float64(intervalBlocks), maxCommitTime)
		fmt.Printf("  State PreCommit - Avg: %.2f ms, Max: %v\n", float64(totalStatePreCommit.Milliseconds())/float64(intervalBlocks), maxStatePreCommit)
		fmt.Printf("  PreCommit breakdown - finalise=%.3fms storageWall=%.3fms storageWork=%.3fms accountUpdate=%.3fms hash=%.3fms; storageObjects=%d slots=%d maxWorkers=%d longestObject=%v/%d slots\n",
			float64(totalIntermediateFinalise)/float64(intervalBlocks)/float64(time.Millisecond),
			float64(totalStorageUpdates)/float64(intervalBlocks)/float64(time.Millisecond),
			float64(totalStorageUpdateWork)/float64(intervalBlocks)/float64(time.Millisecond),
			float64(totalAccountUpdates)/float64(intervalBlocks)/float64(time.Millisecond),
			float64(totalAccountHashes)/float64(intervalBlocks)/float64(time.Millisecond),
			totalStorageUpdateObjects, totalStorageUpdateSlots, maxStorageUpdateWorkers, maxStorageUpdateObject, maxStorageUpdateObjectSlots)
		fmt.Printf("  State PostCommit - Avg: %.2f ms, Max: %v\n", float64(totalPostCommit.Milliseconds())/float64(intervalBlocks), maxPostCommit)
		if cfg.UseBinaryTrie {
			fmt.Printf("  Account state wipe - Avg: %.3f ms, Max: %v, destroyed=%d wiped=%d slots=%d stems=%d codeChunks=%d phases(index=%v stem=%v stage=%v origin=%v)\n",
				float64(totalAccountStateWipe.Microseconds())/1000/float64(intervalBlocks), maxAccountStateWipe,
				totalDestroyedAccounts, totalWipedAccounts, totalWipedStorageSlots, totalWipedStemRecords, totalWipedCodeChunks,
				totalWipeIndexScan, totalWipeStemDelete, totalWipeIndexStage, totalWipeOriginBuild)
		}
		fmt.Printf("  Root Pipeline wall - Avg: %.2f ms, Max: %v\n", float64(totalRootPipelineTime.Milliseconds())/float64(intervalBlocks), maxRootPipelineTime)
		fmt.Printf("  Root Compute charged - Avg: %.2f ms, Max: %v (excludes DB write and first %v async archive wait)\n",
			float64(totalRootComputeTime.Milliseconds())/float64(intervalBlocks), maxRootComputeTime, archiveOverlapBudget)
		fmt.Printf("  Root DB write - Avg: %.2f ms, Max: %v\n", float64(totalRootDBWriteTime.Milliseconds())/float64(intervalBlocks), maxRootDBWriteTime)
		fmt.Printf("  Archive wait over budget - Avg: %.2f ms, Max: %v\n", float64(totalArchiveWaitExcess.Milliseconds())/float64(intervalBlocks), maxArchiveWaitExcess)
		fmt.Printf("  Storage bytes: layout=%s total=%d shared/state=%d archive=%d archiveValid=%v\n", storageLayout, totalStorageSize, stateStorageSize, archiveStorageSize, archiveStorageBytesValid)
		fmt.Printf("  Storage shares: state=%.2f%%, archive=%.2f%%, archiveBytesPerItem=%.2f, stateBytesPerActiveLeaf=%.2f, archiveBytesPerLogicalValue=%.2f, stateBytesPerActiveLogicalValue=%.2f\n",
			stateStorageSharePct, archiveStorageSharePct, archiveBytesPerItem, stateBytesPerActiveLeaf,
			archiveBytesPerLogicalValue, stateBytesPerActiveLogicalValue)
		fmt.Printf("  Memory - HeapAlloc=%dMB HeapSys=%dMB RuntimeSys=%dMB NodeCache=%dMB entries=%d\n",
			memStats.HeapAlloc/(1024*1024),
			memStats.HeapSys/(1024*1024),
			memStats.Sys/(1024*1024),
			commitDiag.NodeCacheBytes/(1024*1024),
			commitDiag.NodeCacheEntries,
		)
		if cfg.UseBinaryTrie {
			fmt.Printf("  Node cache - window hits=%d misses=%d evictions=%d; lifetime hits=%d misses=%d evictions=%d\n",
				cacheWindowHits, cacheWindowMisses, cacheWindowEvictions,
				commitDiag.NodeCacheTotalHits, commitDiag.NodeCacheTotalMisses, commitDiag.NodeCacheEvictions)
			fmt.Printf("  Node cache detail - entries=%d/%d bytes=%d/%d shards=%d windowLockWait=%v contentions=%d dbGets=%d dbTime=%v loaded=%d bytes\n",
				commitDiag.NodeCacheEntries, commitDiag.NodeCacheEntryLimit, commitDiag.NodeCacheBytes, commitDiag.NodeCacheBytesLimit,
				commitDiag.NodeCacheShards, time.Duration(cacheWindowLockWaitNanos), cacheWindowLockContentions,
				cacheWindowDBGets, time.Duration(cacheWindowDBGetNanos), cacheWindowDBLoadBytes)
		}
		if cfg.UseBinaryTrie {
			fmt.Printf("  ASCT Struct - OuterLeaves=%d, ArchiveRecords=%d, ActiveLogicalValues=%d, ArchivedLogicalValues=%d, LogicalReadFailures=%d/%d, Buckets=%d, RootBuckets=%d/%d items (leaf=%d/%d, stub=%d/%d), StubBuckets=%d/%d items (deep=%d/%d), ChildBuckets=%d/%d items, MaxBucketsPath=%d, MaxStubList=%d/%d items (root=%d/%d, deep=%d/%d)\n",
				trieChildNodeCount, totalArchivedItems, activeLogicalValues, archivedLogicalValues, activeLogicalFailures, archiveLogicalFailures, totalBucketCount,
				rootBucketCount, rootArchivedItems,
				rootLeafBucketCount, rootLeafItems, rootStubBucketCount, rootStubItems,
				stubBucketCount, stubArchivedItems,
				deepStubBucketCount, deepStubItems,
				childBucketCount, childArchivedItems,
				maxBucketsPath,
				maxStubListBuckets, maxStubListItems,
				maxRootStubBuckets, maxRootStubItems,
				maxDeepStubBuckets, maxDeepStubItems)
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
		var cumulativeArchive archivetrie.ArchiveCumulativeDiagnostics
		cumulativeArchiveEventRatePct := 0.0
		cumulativeArchivedVsActivePct := 0.0
		if cfg.UseBinaryTrie {
			prunePressure = archivetrie.LastPrunePressureDiagnostics()
			cumulativeArchive = archivetrie.LastArchiveCumulativeDiagnostics()
			if cumulativeArchive.FlatValuePuts > 0 {
				cumulativeArchiveEventRatePct = float64(cumulativeArchive.ArchivedLeaves) * 100 / float64(cumulativeArchive.FlatValuePuts)
			}
			if cumulativeArchive.ArchivedLeaves+trieChildNodeCount > 0 {
				cumulativeArchivedVsActivePct = float64(cumulativeArchive.ArchivedLeaves) * 100 / float64(cumulativeArchive.ArchivedLeaves+trieChildNodeCount)
			}
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
			fmt.Printf("  ASCT Prune Pressure - MaxShard=%d ShardTime=%v Total=%v LockWait=%v RootLoad=%v Walk=%v Finish=%v DetailedCounters=%v Leaves=%d Stubs=%d BuildItems=%d BuildBuckets=%d InternalVisits=%d\n",
				prunePressure.MaxShardID,
				time.Duration(prunePressure.MaxShardNanos),
				time.Duration(prunePressure.MaxTotalNanos),
				time.Duration(prunePressure.MaxLockWaitNanos),
				time.Duration(prunePressure.MaxRootLoadNanos),
				time.Duration(prunePressure.MaxWalkNanos),
				time.Duration(prunePressure.MaxFinishNanos),
				prunePressure.MaxDetailedCountersEnabled,
				prunePressure.MaxLeaves,
				prunePressure.MaxStubs,
				prunePressure.MaxBuildItems,
				prunePressure.MaxBuildBuckets,
				prunePressure.MaxInternalVisits,
			)
			fmt.Printf("  ASCT cumulative archive - ArchivedLeaves=%d FlatPuts=%d FlatDeletes=%d FlatPutBytes=%d EventRate=%.4f%% ArchivedVsActive=%.4f%%\n",
				cumulativeArchive.ArchivedLeaves,
				cumulativeArchive.FlatValuePuts,
				cumulativeArchive.FlatValueDeletes,
				cumulativeArchive.FlatValuePutBytes,
				cumulativeArchiveEventRatePct,
				cumulativeArchivedVsActivePct,
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

		metricsCollectionDuration := time.Since(metricsStart)
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
			strconv.FormatBool(cfg.BinaryStemArchive),
			fmt.Sprintf("%.2f", archiveBytesPerItem),
			fmt.Sprintf("%.2f", stateBytesPerActiveLeaf),
			fmt.Sprintf("%.2f", archiveBytesPerLogicalValue),
			fmt.Sprintf("%.2f", stateBytesPerActiveLogicalValue),
			strconv.FormatInt(trieChildNodeCount, 10),
			strconv.FormatInt(totalArchivedItems, 10),
			strconv.Itoa(totalBucketCount),
			strconv.FormatInt(activeLogicalValues, 10),
			strconv.FormatInt(archivedLogicalValues, 10),
			strconv.FormatInt(activeLogicalFailures, 10),
			strconv.FormatInt(archiveLogicalFailures, 10),
			strconv.Itoa(rootBucketCount),
			strconv.FormatInt(rootArchivedItems, 10),
			strconv.Itoa(rootLeafBucketCount),
			strconv.FormatInt(rootLeafItems, 10),
			strconv.Itoa(stubBucketCount),
			strconv.FormatInt(stubArchivedItems, 10),
			strconv.Itoa(childBucketCount),
			strconv.FormatInt(childArchivedItems, 10),
			strconv.Itoa(rootStubBucketCount),
			strconv.FormatInt(rootStubItems, 10),
			strconv.Itoa(deepStubBucketCount),
			strconv.FormatInt(deepStubItems, 10),
			strconv.Itoa(maxStubListBuckets),
			strconv.FormatInt(maxStubListItems, 10),
			strconv.Itoa(maxRootStubBuckets),
			strconv.FormatInt(maxRootStubItems, 10),
			strconv.Itoa(maxDeepStubBuckets),
			strconv.FormatInt(maxDeepStubItems, 10),
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
			fmt.Sprintf("%.2f", float64(totalStatePreCommit.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxStatePreCommit.Milliseconds(), 10),
			fmt.Sprintf("%.2f", float64(totalPostCommit.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxPostCommit.Milliseconds(), 10),
			fmt.Sprintf("%.2f", float64(totalRootPipelineTime.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxRootPipelineTime.Milliseconds(), 10),
			fmt.Sprintf("%.2f", float64(totalRootComputeTime.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxRootComputeTime.Milliseconds(), 10),
			fmt.Sprintf("%.2f", float64(totalRootDBWriteTime.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxRootDBWriteTime.Milliseconds(), 10),
			fmt.Sprintf("%.2f", float64(totalArchiveWaitExcess.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxArchiveWaitExcess.Milliseconds(), 10),
			strconv.FormatInt(archiveOverlapBudget.Milliseconds(), 10),
			strconv.FormatUint(maxCommitBlock, 10),
			strconv.FormatUint(maxStatePreCommitBlock, 10),
			strconv.FormatUint(maxPostCommitBlock, 10),
			strconv.FormatUint(maxRootPipelineBlock, 10),
			strconv.FormatUint(maxRootComputeBlock, 10),
			strconv.FormatUint(maxRootDBWriteBlock, 10),
			strconv.FormatUint(maxArchiveWaitBlock, 10),
			fmt.Sprintf("%.2f", float64(totalHandleDestruct.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxHandleDestruct.Milliseconds(), 10),
			strconv.FormatUint(maxHandleDestructBlock, 10),
			fmt.Sprintf("%.2f", float64(totalAccountStateWipe.Microseconds())/1000/float64(intervalBlocks)),
			fmt.Sprintf("%.3f", float64(maxAccountStateWipe.Microseconds())/1000),
			strconv.FormatUint(maxAccountWipeBlock, 10),
			strconv.FormatUint(totalDestroyedAccounts, 10),
			strconv.FormatUint(totalWipedAccounts, 10),
			strconv.FormatUint(totalWipedStorageSlots, 10),
			strconv.FormatUint(totalWipedCodeChunks, 10),
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
			strconv.FormatInt(prunePressure.MaxLockWaitNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(prunePressure.MaxRootLoadNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(prunePressure.MaxWalkNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(prunePressure.MaxFinishNanos/int64(time.Microsecond), 10),
			strconv.FormatBool(prunePressure.MaxDetailedCountersEnabled),
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
			strconv.FormatInt(prunePressure.MaxPathAbsorbed, 10),
			strconv.FormatInt(prunePressure.MaxRootPoolItems, 10),
			strconv.FormatInt(prunePressure.MaxRootPoolBuckets, 10),
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
			strconv.FormatInt(cumulativeArchive.ArchivedLeaves, 10),
			strconv.FormatInt(cumulativeArchive.FlatValuePuts, 10),
			strconv.FormatInt(cumulativeArchive.FlatValueDeletes, 10),
			strconv.FormatInt(cumulativeArchive.FlatValuePutBytes, 10),
			fmt.Sprintf("%.4f", cumulativeArchiveEventRatePct),
			fmt.Sprintf("%.4f", cumulativeArchivedVsActivePct),
			storageLayout,
			strconv.FormatInt(stateStorageSize, 10),
			strconv.FormatBool(archiveStorageBytesValid),
			fmt.Sprintf("%.3f", float64(totalTxTime)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(maxTxTime)/float64(time.Millisecond)),
			strconv.FormatUint(maxTxTimeBlock, 10),
			strconv.FormatUint(intervalTxCount, 10),
			strconv.FormatUint(intervalSuccessTxCount, 10),
			fmt.Sprintf("%.3f", txExecutionTPS),
			fmt.Sprintf("%.3f", float64(metricsCollectionDuration)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(stateDirScanDuration)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(archiveDirScanDuration)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(trieStatsDuration)/float64(time.Millisecond)),
			strconv.FormatBool(trieStatsExact),
			strconv.FormatUint(lastFullTrieStatsBlock, 10),
			strconv.FormatInt(commitDiag.NodeCacheTotalHits, 10),
			strconv.FormatInt(commitDiag.NodeCacheTotalMisses, 10),
			strconv.FormatInt(commitDiag.NodeCacheEvictions, 10),
			strconv.FormatInt(cacheWindowHits, 10),
			strconv.FormatInt(cacheWindowMisses, 10),
			strconv.FormatInt(cacheWindowEvictions, 10),
			fmt.Sprintf("%.3f", float64(totalHashTotal)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(totalHashShardWall)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(totalHashShardWork)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(totalHashSerialize)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(totalHashCompute)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(totalHashRootMerge)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(maxHashTotal)/float64(time.Millisecond)),
			strconv.FormatUint(maxHashBlock, 10),
			fmt.Sprintf("%.3f", float64(totalHashDirtyShards)/float64(intervalBlocks)),
			fmt.Sprintf("%.3f", float64(totalHashWorkers)/float64(intervalBlocks)),
			fmt.Sprintf("%.3f", float64(totalHashNodes)/float64(intervalBlocks)),
			fmt.Sprintf("%.3f", float64(totalHashDirtyNodes)/float64(intervalBlocks)),
			fmt.Sprintf("%.3f", float64(totalHashCleanNodes)/float64(intervalBlocks)),
			strconv.FormatUint(totalWipedStemRecords, 10),
			fmt.Sprintf("%.3f", float64(totalWipeIndexScan)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(totalWipeStemDelete)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(totalWipeIndexStage)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(totalWipeOriginBuild)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(totalIntermediateFinalise)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(maxIntermediateFinalise)/float64(time.Millisecond)),
			strconv.FormatUint(maxIntermediateFinaliseBlock, 10),
			fmt.Sprintf("%.3f", float64(totalStorageUpdates)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(maxStorageUpdates)/float64(time.Millisecond)),
			strconv.FormatUint(maxStorageUpdatesBlock, 10),
			fmt.Sprintf("%.3f", float64(totalStorageUpdateWork)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(maxStorageUpdateObject)/float64(time.Millisecond)),
			strconv.FormatUint(maxStorageUpdateObjectBlock, 10),
			strconv.FormatInt(maxStorageUpdateObjectSlots, 10),
			fmt.Sprintf("%.3f", float64(totalStorageUpdateObjects)/float64(intervalBlocks)),
			fmt.Sprintf("%.3f", float64(totalStorageUpdateSlots)/float64(intervalBlocks)),
			strconv.FormatInt(maxStorageUpdateWorkers, 10),
			fmt.Sprintf("%.3f", float64(totalAccountUpdates)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(maxAccountUpdates)/float64(time.Millisecond)),
			strconv.FormatUint(maxAccountUpdatesBlock, 10),
			fmt.Sprintf("%.3f", float64(totalAccountHashes)/float64(intervalBlocks)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(maxAccountHashes)/float64(time.Millisecond)),
			strconv.FormatUint(maxAccountHashesBlock, 10),
			strconv.FormatInt(totalIntermediateMutations, 10),
			strconv.FormatInt(totalAccountUpdated, 10),
			strconv.FormatInt(totalAccountDeleted, 10),
			strconv.FormatInt(totalStorageUpdated, 10),
			strconv.FormatInt(totalStorageDeleted, 10),
			strconv.FormatInt(windowUpdateDiagnostics.StemPutCalls, 10),
			strconv.FormatInt(windowUpdateDiagnostics.StemPutNoops, 10),
			strconv.FormatInt(windowUpdateDiagnostics.StemPutLoadedBytes, 10),
			strconv.FormatInt(windowUpdateDiagnostics.StemPutEncodedBytes, 10),
			strconv.FormatInt(windowUpdateDiagnostics.StemPutCommitmentHashes, 10),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.StemPutTotalNanos)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.StemPutLoadNanos)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.StemPutDecodeNanos)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.StemPutEncodeNanos)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.StemPutBackendNanos)/float64(time.Millisecond)),
			strconv.FormatInt(windowUpdateDiagnostics.StemApplyCalls, 10),
			strconv.FormatInt(windowUpdateDiagnostics.StemApplyUpdates, 10),
			strconv.FormatInt(windowUpdateDiagnostics.StemApplyStems, 10),
			strconv.FormatInt(windowUpdateDiagnostics.StemApplyPuts, 10),
			strconv.FormatInt(windowUpdateDiagnostics.StemApplyDeletes, 10),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.StemApplyTotalNanos)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.StemApplyLoadNanos)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.StemApplyEncodeNanos)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.StemApplyBackendNanos)/float64(time.Millisecond)),
			strconv.FormatInt(windowUpdateDiagnostics.ShardPutCalls, 10),
			strconv.FormatInt(windowUpdateDiagnostics.ShardPutValues, 10),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.ShardPutNanos)/float64(time.Millisecond)),
			strconv.FormatInt(windowUpdateDiagnostics.ShardPutBatchCalls, 10),
			strconv.FormatInt(windowUpdateDiagnostics.ShardPutBatchValues, 10),
			strconv.FormatInt(windowUpdateDiagnostics.ShardPutBatchShards, 10),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.ShardPutBatchWallNanos)/float64(time.Millisecond)),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.ShardPutBatchWorkNanos)/float64(time.Millisecond)),
			strconv.FormatInt(windowUpdateDiagnostics.ShardDeleteCalls, 10),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.ShardDeleteNanos)/float64(time.Millisecond)),
			strconv.FormatInt(windowUpdateDiagnostics.FlatValueGets, 10),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.FlatValueReadIONanos)/float64(time.Millisecond)),
			strconv.FormatInt(windowUpdateDiagnostics.FlatValueReadIOBytes, 10),
			strconv.FormatInt(windowUpdateDiagnostics.FlatValuePuts, 10),
			strconv.FormatInt(windowUpdateDiagnostics.FlatValueDeletes, 10),
			fmt.Sprintf("%.3f", float64(windowUpdateDiagnostics.FlatValueWriteNanos)/float64(time.Millisecond)),
			strconv.FormatInt(windowUpdateDiagnostics.ArchivePromotionChecks, 10),
			strconv.FormatInt(windowUpdateDiagnostics.ArchivePromotionHits, 10),
			strconv.FormatInt(commitDiag.NodeCacheEntryLimit, 10),
			strconv.FormatInt(commitDiag.NodeCacheBytesLimit, 10),
			strconv.FormatInt(commitDiag.NodeCacheShards, 10),
			strconv.FormatInt(cacheWindowLockContentions, 10),
			fmt.Sprintf("%.3f", float64(cacheWindowLockWaitNanos)/float64(time.Millisecond)),
			strconv.FormatInt(cacheWindowDBGets, 10),
			fmt.Sprintf("%.3f", float64(cacheWindowDBGetNanos)/float64(time.Millisecond)),
			strconv.FormatInt(cacheWindowDBLoadBytes, 10),
		}
		if len(record) != len(metricsHeader) {
			t.Fatalf("metrics CSV column mismatch: header=%d record=%d", len(metricsHeader), len(record))
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
		totalStatePreCommit = 0
		maxStatePreCommit = 0
		maxStatePreCommitBlock = 0
		totalIntermediateFinalise = 0
		maxIntermediateFinalise = 0
		maxIntermediateFinaliseBlock = 0
		totalStorageUpdates = 0
		maxStorageUpdates = 0
		maxStorageUpdatesBlock = 0
		totalStorageUpdateWork = 0
		maxStorageUpdateObject = 0
		maxStorageUpdateObjectBlock = 0
		maxStorageUpdateObjectSlots = 0
		totalStorageUpdateObjects = 0
		totalStorageUpdateSlots = 0
		maxStorageUpdateWorkers = 0
		totalAccountUpdates = 0
		maxAccountUpdates = 0
		maxAccountUpdatesBlock = 0
		totalAccountHashes = 0
		maxAccountHashes = 0
		maxAccountHashesBlock = 0
		totalIntermediateMutations = 0
		totalAccountUpdated = 0
		totalAccountDeleted = 0
		totalStorageUpdated = 0
		totalStorageDeleted = 0
		totalPostCommit = 0
		maxPostCommit = 0
		maxPostCommitBlock = 0
		totalRootPipelineTime = 0
		maxRootPipelineTime = 0
		maxRootPipelineBlock = 0
		totalRootComputeTime = 0
		maxRootComputeTime = 0
		maxRootComputeBlock = 0
		totalRootDBWriteTime = 0
		maxRootDBWriteTime = 0
		maxRootDBWriteBlock = 0
		totalHandleDestruct = 0
		maxHandleDestruct = 0
		maxHandleDestructBlock = 0
		totalAccountStateWipe = 0
		maxAccountStateWipe = 0
		maxAccountWipeBlock = 0
		totalDestroyedAccounts = 0
		totalWipedAccounts = 0
		totalWipedStorageSlots = 0
		totalWipedCodeChunks = 0
		totalWipedStemRecords = 0
		totalWipeIndexScan = 0
		totalWipeStemDelete = 0
		totalWipeIndexStage = 0
		totalWipeOriginBuild = 0
		totalPruneTime = 0
		maxPruneTime = 0
		maxPruneBlock = 0
		totalArchiveCompute = 0
		maxArchiveCompute = 0
		totalArchiveWait = 0
		maxArchiveWait = 0
		totalArchiveWaitExcess = 0
		maxArchiveWaitExcess = 0
		maxArchiveWaitBlock = 0
		maxProofSizeBlockBlock = 0
		pruneCount = 0
		totalTxTime = 0 // Reset Tx Execution stats
		maxTxTime = 0   // Reset Tx Execution stats
		maxTxTimeBlock = 0
		intervalTxCount = 0
		intervalSuccessTxCount = 0
		totalHashShardWall = 0
		totalHashTotal = 0
		totalHashShardWork = 0
		totalHashSerialize = 0
		totalHashCompute = 0
		totalHashRootMerge = 0
		maxHashTotal = 0
		maxHashBlock = 0
		totalHashDirtyShards = 0
		totalHashWorkers = 0
		totalHashNodes = 0
		totalHashDirtyNodes = 0
		totalHashCleanNodes = 0

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
			strconv.FormatInt(p.LastLockWaitNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(p.LastRootLoadNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(p.LastWalkNanos/int64(time.Microsecond), 10),
			strconv.FormatInt(p.LastFinishNanos/int64(time.Microsecond), 10),
			strconv.FormatBool(p.DetailedCountersEnabled),
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
			strconv.FormatInt(p.LastPathAbsorbed, 10),
			strconv.FormatInt(p.LastRootPoolItems, 10),
			strconv.FormatInt(p.LastRootPoolBuckets, 10),
		})
		if block%1000 == 0 {
			pruneShardWriter.Flush()
		}
	}
	type rootTimingBreakdown struct {
		computeCharged        time.Duration
		dbWrite               time.Duration
		archiveWaitOverBudget time.Duration
	}
	recordArchiveAfterCommit := func(block uint64, pruned bool, rootDuration time.Duration, codeWriteDuration time.Duration) rootTimingBreakdown {
		commitDiag := archivetrie.LastCommitDiagnostics()
		breakdown := rootTimingBreakdown{
			computeCharged: rootDuration,
			dbWrite:        codeWriteDuration,
		}
		if cfg.UseBinaryTrie {
			breakdown.dbWrite += time.Duration(commitDiag.BatchWriteNanos)
		}
		if breakdown.dbWrite > 0 {
			if breakdown.dbWrite >= breakdown.computeCharged {
				breakdown.computeCharged = 0
			} else {
				breakdown.computeCharged -= breakdown.dbWrite
			}
		}
		if !pruned || !cfg.UseBinaryTrie {
			return breakdown
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
		if cfg.BinaryAsyncPrune {
			freeWait := archiveWait
			if freeWait > archiveOverlapBudget {
				freeWait = archiveOverlapBudget
				breakdown.archiveWaitOverBudget = archiveWait - archiveOverlapBudget
			}
			if freeWait > 0 {
				if freeWait >= breakdown.computeCharged {
					breakdown.computeCharged = 0
				} else {
					breakdown.computeCharged -= freeWait
				}
			}
			if breakdown.archiveWaitOverBudget > 0 {
				fmt.Printf("[ASCT_ARCHIVE_WAIT_BUDGET] block=%d wait=%v budget=%v over=%v archiveCompute=%v\n",
					block, archiveWait, archiveOverlapBudget, breakdown.archiveWaitOverBudget, archiveCompute)
			}
		}
		recordPruneShardMetrics(block)
		if cfg.BinaryAsyncPrune {
			if cfg.MaxPruningMs > 0 && archiveCompute > time.Duration(cfg.MaxPruningMs)*time.Millisecond {
				fmt.Printf("[ASCT_ARCHIVE_COMPUTE_LIMIT] block=%d compute=%v limit=%v mode=async action=warn\n",
					block, archiveCompute, time.Duration(cfg.MaxPruningMs)*time.Millisecond)
			}
		} else {
			checkDurationLimit(t, "binary archive compute", block, archiveCompute, cfg.MaxPruningMs)
		}
		return breakdown
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
				recordPreCommitBreakdown(b, statedb)
				var hashDiag archivetrie.HashDiagnostics
				if cfg.UseBinaryTrie {
					hashDiag = archivetrie.LastHashDiagnostics()
					recordHashBlockStats(b, hashDiag)
				}
				postCommitStart := time.Now()
				h, err := statedb.PostCommit(b, false, false)
				if err != nil {
					t.Fatalf("post-commit failed at empty block %d: %v", b, err)
				}
				postCommitDuration := time.Since(postCommitStart)
				commitDuration := time.Since(commitStart)
				rootDuration := time.Since(rootStart)
				recordKVBlockStats(b)
				if commitDuration >= slowCommitDiagThreshold || statedb.CommitHandleDestruction >= slowCommitDiagThreshold || statedb.CommitAccountStateWipe >= slowCommitDiagThreshold {
					treeLabel := "MPT"
					extra := ""
					if cfg.UseBinaryTrie {
						treeLabel = "ASCT"
						extra = " " + hashDiag.String() + " " + archivetrie.LastCommitDiagnostics().String() + " " + archivetrie.LastPrunePressureDiagnostics().String()
					} else if cfg.UseVerkle {
						treeLabel = "Verkle"
					}
					fmt.Printf("[%s_COMMIT_DIAG] block=%d empty=true commit=%v pre=%v post=%v commitInternal=%v accountStateWipe=%v destroyedAccounts=%d wipedAccounts=%d wipedSlots=%d wipedStems=%d wipedCodeChunks=%d wipeIndexScan=%v wipeStemDelete=%v wipeIndexStage=%v wipeOriginBuild=%v handleDestruction=%v deleteMerge=%v workers=%v afterWorkers=%v buildUpdate=%v codeWrite=%v accountCommit=%v storageCommit=%v snapshotCommit=%v trieDBCommit=%v readerReset=%v root_pipeline=%v%s\n",
						treeLabel, b, commitDuration, preCommitDuration, postCommitDuration, statedb.CommitInternal, statedb.CommitAccountStateWipe, statedb.CommitDestroyedAccounts, statedb.CommitWipedAccounts, statedb.CommitWipedStorageSlots, statedb.CommitWipedStemRecords, statedb.CommitWipedCodeChunks, statedb.CommitWipeIndexScan, statedb.CommitWipeStemDelete, statedb.CommitWipeIndexStage, statedb.CommitWipeOriginBuild, statedb.CommitHandleDestruction, statedb.CommitDeleteMerge, statedb.CommitWorkers, statedb.CommitAfterWorkers, statedb.CommitBuildUpdate, statedb.CommitCodeWrite, statedb.AccountCommits, statedb.StorageCommits, statedb.SnapshotCommits, statedb.TrieDBCommits, statedb.CommitReaderReset, rootDuration, extra)
				}
				checkDurationLimit(t, "accountStateWipe", b, statedb.CommitAccountStateWipe, cfg.MaxHandleDestructMs)
				checkDurationLimit(t, "handleDestruction", b, statedb.CommitHandleDestruction, cfg.MaxHandleDestructMs)
				rootTiming := recordArchiveAfterCommit(b, prunedThisBlock, rootDuration, statedb.CommitCodeWrite)
				checkDurationLimit(t, "root compute charged", b, rootTiming.computeCharged, cfg.MaxRootPipelineMs)
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
				totalStatePreCommit += preCommitDuration
				if preCommitDuration > maxStatePreCommit {
					maxStatePreCommit = preCommitDuration
					maxStatePreCommitBlock = b
				}
				totalPostCommit += postCommitDuration
				if postCommitDuration > maxPostCommit {
					maxPostCommit = postCommitDuration
					maxPostCommitBlock = b
				}
				totalHandleDestruct += statedb.CommitHandleDestruction
				if statedb.CommitHandleDestruction > maxHandleDestruct {
					maxHandleDestruct = statedb.CommitHandleDestruction
					maxHandleDestructBlock = b
				}
				totalAccountStateWipe += statedb.CommitAccountStateWipe
				if statedb.CommitAccountStateWipe > maxAccountStateWipe {
					maxAccountStateWipe = statedb.CommitAccountStateWipe
					maxAccountWipeBlock = b
				}
				totalDestroyedAccounts += uint64(statedb.CommitDestroyedAccounts)
				totalWipedAccounts += uint64(statedb.CommitWipedAccounts)
				totalWipedStorageSlots += uint64(statedb.CommitWipedStorageSlots)
				totalWipedCodeChunks += uint64(statedb.CommitWipedCodeChunks)
				totalWipedStemRecords += uint64(statedb.CommitWipedStemRecords)
				totalWipeIndexScan += statedb.CommitWipeIndexScan
				totalWipeStemDelete += statedb.CommitWipeStemDelete
				totalWipeIndexStage += statedb.CommitWipeIndexStage
				totalWipeOriginBuild += statedb.CommitWipeOriginBuild
				totalRootPipelineTime += rootDuration
				if rootDuration > maxRootPipelineTime {
					maxRootPipelineTime = rootDuration
					maxRootPipelineBlock = b
				}
				totalRootComputeTime += rootTiming.computeCharged
				if rootTiming.computeCharged > maxRootComputeTime {
					maxRootComputeTime = rootTiming.computeCharged
					maxRootComputeBlock = b
				}
				totalRootDBWriteTime += rootTiming.dbWrite
				if rootTiming.dbWrite > maxRootDBWriteTime {
					maxRootDBWriteTime = rootTiming.dbWrite
					maxRootDBWriteBlock = b
				}
				totalArchiveWaitExcess += rootTiming.archiveWaitOverBudget
				if rootTiming.archiveWaitOverBudget > maxArchiveWaitExcess {
					maxArchiveWaitExcess = rootTiming.archiveWaitOverBudget
					maxArchiveWaitBlock = b
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
					reportStats(false)
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
				maxTxTimeBlock = b
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
			recordPreCommitBreakdown(b, statedb)
			var hashDiag archivetrie.HashDiagnostics
			if cfg.UseBinaryTrie {
				hashDiag = archivetrie.LastHashDiagnostics()
				recordHashBlockStats(b, hashDiag)
			}
			postCommitStart := time.Now()
			h, err := statedb.PostCommit(b, false, false)
			if err != nil {
				t.Fatalf("post-commit failed at block %d: %v", b, err)
			}
			postCommitDuration := time.Since(postCommitStart)
			commitDuration := time.Since(commitStart)

			rootDuration := time.Since(rootStart)
			recordKVBlockStats(b)
			if commitDuration >= slowCommitDiagThreshold || statedb.CommitHandleDestruction >= slowCommitDiagThreshold || statedb.CommitAccountStateWipe >= slowCommitDiagThreshold {
				treeLabel := "MPT"
				extra := ""
				if cfg.UseBinaryTrie {
					treeLabel = "ASCT"
					extra = " " + hashDiag.String() + " " + archivetrie.LastCommitDiagnostics().String() + " " + archivetrie.LastPrunePressureDiagnostics().String()
				} else if cfg.UseVerkle {
					treeLabel = "Verkle"
				}
				fmt.Printf("[%s_COMMIT_DIAG] block=%d empty=false commit=%v pre=%v post=%v commitInternal=%v accountStateWipe=%v destroyedAccounts=%d wipedAccounts=%d wipedSlots=%d wipedStems=%d wipedCodeChunks=%d wipeIndexScan=%v wipeStemDelete=%v wipeIndexStage=%v wipeOriginBuild=%v handleDestruction=%v deleteMerge=%v workers=%v afterWorkers=%v buildUpdate=%v codeWrite=%v accountCommit=%v storageCommit=%v snapshotCommit=%v trieDBCommit=%v readerReset=%v root_pipeline=%v%s\n",
					treeLabel, b, commitDuration, preCommitDuration, postCommitDuration, statedb.CommitInternal, statedb.CommitAccountStateWipe, statedb.CommitDestroyedAccounts, statedb.CommitWipedAccounts, statedb.CommitWipedStorageSlots, statedb.CommitWipedStemRecords, statedb.CommitWipedCodeChunks, statedb.CommitWipeIndexScan, statedb.CommitWipeStemDelete, statedb.CommitWipeIndexStage, statedb.CommitWipeOriginBuild, statedb.CommitHandleDestruction, statedb.CommitDeleteMerge, statedb.CommitWorkers, statedb.CommitAfterWorkers, statedb.CommitBuildUpdate, statedb.CommitCodeWrite, statedb.AccountCommits, statedb.StorageCommits, statedb.SnapshotCommits, statedb.TrieDBCommits, statedb.CommitReaderReset, rootDuration, extra)
			}
			checkDurationLimit(t, "accountStateWipe", b, statedb.CommitAccountStateWipe, cfg.MaxHandleDestructMs)
			checkDurationLimit(t, "handleDestruction", b, statedb.CommitHandleDestruction, cfg.MaxHandleDestructMs)
			rootTiming := recordArchiveAfterCommit(b, prunedThisBlock, rootDuration, statedb.CommitCodeWrite)
			checkDurationLimit(t, "root compute charged", b, rootTiming.computeCharged, cfg.MaxRootPipelineMs)

			if b%100000 == 0 {
				fmt.Printf("[测试] block %d: finalise=%v, pre=%v, post=%v, commit=%v, root_pipeline=%v\n",
					b, finaliseDuration, preCommitDuration, postCommitDuration, commitDuration, rootDuration)
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
			totalStatePreCommit += preCommitDuration
			if preCommitDuration > maxStatePreCommit {
				maxStatePreCommit = preCommitDuration
				maxStatePreCommitBlock = b
			}
			totalPostCommit += postCommitDuration
			if postCommitDuration > maxPostCommit {
				maxPostCommit = postCommitDuration
				maxPostCommitBlock = b
			}
			totalHandleDestruct += statedb.CommitHandleDestruction
			if statedb.CommitHandleDestruction > maxHandleDestruct {
				maxHandleDestruct = statedb.CommitHandleDestruction
				maxHandleDestructBlock = b
			}
			totalAccountStateWipe += statedb.CommitAccountStateWipe
			if statedb.CommitAccountStateWipe > maxAccountStateWipe {
				maxAccountStateWipe = statedb.CommitAccountStateWipe
				maxAccountWipeBlock = b
			}
			totalDestroyedAccounts += uint64(statedb.CommitDestroyedAccounts)
			totalWipedAccounts += uint64(statedb.CommitWipedAccounts)
			totalWipedStorageSlots += uint64(statedb.CommitWipedStorageSlots)
			totalWipedCodeChunks += uint64(statedb.CommitWipedCodeChunks)
			totalWipedStemRecords += uint64(statedb.CommitWipedStemRecords)
			totalWipeIndexScan += statedb.CommitWipeIndexScan
			totalWipeStemDelete += statedb.CommitWipeStemDelete
			totalWipeIndexStage += statedb.CommitWipeIndexStage
			totalWipeOriginBuild += statedb.CommitWipeOriginBuild
			totalRootPipelineTime += rootDuration
			if rootDuration > maxRootPipelineTime {
				maxRootPipelineTime = rootDuration
				maxRootPipelineBlock = b
			}
			totalRootComputeTime += rootTiming.computeCharged
			if rootTiming.computeCharged > maxRootComputeTime {
				maxRootComputeTime = rootTiming.computeCharged
				maxRootComputeBlock = b
			}
			totalRootDBWriteTime += rootTiming.dbWrite
			if rootTiming.dbWrite > maxRootDBWriteTime {
				maxRootDBWriteTime = rootTiming.dbWrite
				maxRootDBWriteBlock = b
			}
			totalArchiveWaitExcess += rootTiming.archiveWaitOverBudget
			if rootTiming.archiveWaitOverBudget > maxArchiveWaitExcess {
				maxArchiveWaitExcess = rootTiming.archiveWaitOverBudget
				maxArchiveWaitBlock = b
			}

			intervalBlocks++
			totalProcessedBlocks++
			currentBlock++
			if intervalBlocks >= statsIv {
				reportStats(false)
			}
			// 每 10w 区块刷新一次假阳性分布
			if (b+1)%100000 == 0 {
				flushGlobalFPDistribution(outputDir)
			}
		}
	}
	// 最后不足一个周期的统计报告
	if intervalBlocks > 0 {
		reportStats(true)
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
		FullTrieStatsInterval:       *fullTrieStatsInterval,
		ShardDepth:                  *shardDepth,
		ArchiveBucketSize:           *archiveBucketSize,
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
		BinaryStemArchive:           *binaryStemArchive,
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
