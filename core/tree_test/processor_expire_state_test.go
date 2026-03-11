package tree

import (
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"hash"
	"math/big"
	"os"
	"path/filepath"
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
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

// ProcessorHost encapsulates the environment for state processing experiments.
type ProcessorHost struct {
	db     ethdb.Database
	trieDB *triedb.Database
	sdb    *state.CachingDB
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
	useMemory        = flag.Bool("useMemory2", false, "Use in-memory DB")
	binaryArchiveDir = flag.String("binaryArchiveDir2", "F:\\expire_data\\expire_state_db_achive", "Binary trie archive directory")
	statsInterval    = flag.Int("statsInterval2", 100000, "Statistics reporting interval (in blocks)")
	pruneInterval    = flag.Int("pruneInterval", 5, "Blocks between Trie.PruneNextShard() calls")
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
	UseMemory        bool
	BinaryArchiveDir string
	StartNum         uint64
	PruneInterval    int
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
		PathDB:           pdb,
		HashDB:           hdb,
	})

	firstRootHash := types.EmptyRootHash
	if cfg.UseVerkle || cfg.UseBinaryTrie {
		firstRootHash = common.Hash{}
	}
	// --- 硬编码开关：当使用 Binary Trie 时是否禁用快照 (用于精准调试 Binary Trie 指标) ---
	disableSnapForBinary := true
	var activeSnaps *snapshot.Tree
	if !(cfg.UseBinaryTrie && disableSnapForBinary) {
		activeSnaps, _ = snapshot.New(snapshot.Config{CacheSize: 100}, db, trieDB, firstRootHash)
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

// Transaction loading is now handled by TransactionStreamer in processor_utils.go

// 增加配置 -- binary-trie的分片数 binary-trie stub桶的大小上限.
// mpt测试模式下,让mpt会剪枝,不保留历史数据.
func TestExpireStateProcessor(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}
	cfg := &ProcessorConfig{
		DbDir:            *dbDir,
		DataDir:          *dataDir,
		StartFileIdx:     *startIdx,
		EndFileIdx:       *endIdx,
		UseVerkle:        *useVerkle,
		UseBinaryTrie:    *useBinaryTrie,
		UseMemory:        *useMemory,
		BinaryArchiveDir: *binaryArchiveDir,
		StartNum:         46147,
		PruneInterval:    *pruneInterval,
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
	lastStateRoot := types.EmptyRootHash
	if cfg.UseVerkle || cfg.UseBinaryTrie {
		lastStateRoot = common.Hash{}
	}

	// Statistics tracking
	statsIv := uint64(*statsInterval)
	var (
		intervalBlocks         uint64
		totalProcessedBlocks   uint64
		epochID                uint64
		totalTxTime            time.Duration
		maxTxTime              time.Duration
		totalRootTime          time.Duration
		maxRootTime            time.Duration
		totalPruneTime         time.Duration
		maxPruneTime           time.Duration
		pruneCount             uint64
		totalStorageSize       int64 // Cumulative storage size
		intervalTxCount        uint64
		intervalSuccessTxCount uint64
	)

	// CSV file setup
	csvFile, err := os.Create("asct_mainnet_metrics.csv")
	if err != nil {
		t.Fatalf("failed to create csv file: %v", err)
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write CSV Header
	writer.Write([]string{
		"Epoch_ID", "Cumulative_Storage_Bytes", "Avg_Root_Calc_Time_ms", "Max_Root_Calc_Time_ms",
		"Avg_Pruning_Time_us", "Max_Pruning_Time_us",
		"Hit_Count", "Miss_NonExistent_Count", "Miss_Existent_Count",
		"Avg_Proof_Gen_Time_ms", "Max_Proof_Gen_Time_ms", "Avg_Proof_Verify_Time_ms", "Max_Proof_Verify_Time_ms",
		"Avg_Proof_Size_Byte", "Max_Proof_Size_Byte",
		"Item_Proof_Min", "Item_Proof_P25", "Item_Proof_Med", "Item_Proof_P75", "Item_Proof_Max",
		"Cycle_FP_Count", "Max_FP_In_Single_Block",
	})

	reportStats := func() {
		if intervalBlocks == 0 {
			return
		}
		epochID++
		storageSize, _ := getDirSize(cfg.DbDir)
		totalStorageSize = storageSize // Update cumulative storage size

		fmt.Printf("  Blocks: %d - %d (Processed Blocks Count)\n", totalProcessedBlocks-intervalBlocks, totalProcessedBlocks-1)
		fmt.Printf("  Tx Execution   - Avg: %v, Max: %v\n", totalTxTime/time.Duration(intervalBlocks), maxTxTime)
		if intervalTxCount > 0 {
			fmt.Printf("  Tx Success Rate - %.2f%% (%d/%d)\n", float64(intervalSuccessTxCount)*100/float64(intervalTxCount), intervalSuccessTxCount, intervalTxCount)
		}
		fmt.Printf("  平均根计算耗时: %.2f ms\n", float64(totalRootTime.Milliseconds())/float64(intervalBlocks))
		fmt.Printf("  最大根计算耗时: %v\n", maxRootTime)
		fmt.Printf("  累计存储占用: %d 字节\n", totalStorageSize)

		avgBinaryPruneTime := 0.0
		if pruneCount > 0 {
			avgBinaryPruneTime = float64(totalPruneTime) / float64(pruneCount) / float64(time.Microsecond) // ns -> us
		}
		fmt.Printf("  平均二进制裁剪耗时: %.2f us\n", avgBinaryPruneTime)
		fmt.Printf("  最大二进制裁剪耗时: %.2f us\n", float64(maxPruneTime)/float64(time.Microsecond))
		fmt.Printf("  命中热状态次数: %d\n", atomic.LoadInt64(&common.BinaryHitCount))
		fmt.Printf("  未命中且数据不存在次数: %d\n", atomic.LoadInt64(&common.BinaryMissNonExistentCount))
		fmt.Printf("  未命中但数据存在次数: %d\n", atomic.LoadInt64(&common.BinaryMissExistentCount))

		totalReads := atomic.LoadInt64(&common.BinaryHitCount) + atomic.LoadInt64(&common.BinaryMissNonExistentCount) + atomic.LoadInt64(&common.BinaryMissExistentCount)
		avgGenTime := 0.0
		if totalReads > 0 {
			avgGenTime = (float64(atomic.LoadInt64(&common.BinaryProofGenTime)) / float64(totalReads)) / 1_000_000.0 // us -> ms
		}
		maxGenTime := float64(atomic.LoadInt64(&common.BinaryProofGenTimeMax)) / 1_000_000.0

		avgVerifTime := 0.0
		if atomic.LoadInt64(&common.BinaryMissExistentCount) > 0 {
			avgVerifTime = (float64(atomic.LoadInt64(&common.BinaryProofVerifTime)) / float64(atomic.LoadInt64(&common.BinaryMissExistentCount))) / 1_000_000.0 // us -> ms
		}
		maxVerifTime := float64(atomic.LoadInt64(&common.BinaryProofVerifTimeMax)) / 1_000_000.0

		fmt.Printf("  平均证明生成耗时: %.4f ms\n", avgGenTime)
		fmt.Printf("  最大证明生成耗时: %.4f ms\n", maxGenTime)
		fmt.Printf("  平均复活验证耗时: %.4f ms\n", avgVerifTime)
		fmt.Printf("  最大复活验证耗时: %.4f ms\n", maxVerifTime)

		// Proof size metrics
		avgProofSizeBlock := float64(atomic.LoadInt64(&common.BinaryTotalProofSize)) / float64(intervalBlocks)
		maxProofSizeBlock := atomic.LoadInt64(&common.BinaryBlockProofSizeMax)
		fmt.Printf("  平均每区块证明大小: %.2f bytes\n", avgProofSizeBlock)
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
			strconv.FormatInt(storageSize, 10),
			fmt.Sprintf("%.2f", float64(totalRootTime.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxRootTime.Milliseconds(), 10),
			fmt.Sprintf("%.2f", avgBinaryPruneTime),
			strconv.FormatInt(maxPruneTime.Microseconds(), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryHitCount), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryMissNonExistentCount), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryMissExistentCount), 10),
			fmt.Sprintf("%.4f", avgGenTime),
			fmt.Sprintf("%.4f", maxGenTime),
			fmt.Sprintf("%.4f", avgVerifTime),
			fmt.Sprintf("%.4f", maxVerifTime),
			fmt.Sprintf("%.2f", avgProofSizeBlock),
			strconv.FormatInt(maxProofSizeBlock, 10),
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
		totalRootTime = 0
		maxRootTime = 0
		totalPruneTime = 0
		maxPruneTime = 0
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
	}

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
				b := currentBlock

				if b%100000 == 0 {
					fmt.Printf("[测试] 正在处理区块 %d (总计已处理: %d) 耗时: %v... (空块)\n", b, totalProcessedBlocks, time.Since(start10k))
					start10k = time.Now()
				}

				host.trieDB.UpdateBlockNum(b)
				if host.trieDB.CacheTrie() != nil {
					host.trieDB.CacheTrie().SetBlockNum(b)
				}
				statedb, _ := state.New(lastStateRoot, host.sdb)

				// 0. Reward miner/packer
				if miner, ok := compareBlockMiners[b]; ok && miner != (common.Address{}) {
					reward, _ := uint256.FromBig(new(big.Int).Mul(big.NewInt(1e6), big.NewInt(1e18))) // 1,000,000 ETH
					statedb.AddBalance(miner, reward, tracing.BalanceChangeUnspecified)
				}

				if (cfg.UseBinaryTrie || !cfg.UseVerkle) && cfg.PruneInterval > 0 && b%uint64(cfg.PruneInterval) == 0 {
					statedb.PruneNextShard()
				}
				statedb.Finalise(false)
				h, _ := statedb.Commit(b, false, false)
				lastStateRoot = h
				if b%1000 == 0 {
					host.trieDB.Commit(h, false)
				}
				intervalBlocks++
				totalProcessedBlocks++
				currentBlock++
				if intervalBlocks >= statsIv {
					reportStats()
				}
			}

			// Process the block with transactions
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
			statedb, _ := state.New(lastStateRoot, host.sdb)

			// 0. Reward miner/packer
			if miner, ok := compareBlockMiners[b]; ok && miner != (common.Address{}) {
				reward, _ := uint256.FromBig(new(big.Int).Mul(big.NewInt(1e6), big.NewInt(1e18))) // 1,000,000 ETH
				statedb.AddBalance(miner, reward, tracing.BalanceChangeUnspecified)
			}

			blockCtx := vm.BlockContext{
				CanTransfer: core.CanTransfer,
				Transfer:    core.Transfer,
				GetHash:     func(n uint64) common.Hash { return common.Hash{} },
				Coinbase:    compareBlockMiners[b],
				BlockNumber: new(big.Int).SetUint64(b),
				Time:        b * 15,
				Difficulty:  big.NewInt(1),
				BaseFee:     big.NewInt(0),
			}
			evm := vm.NewEVM(blockCtx, statedb, params.MainnetChainConfig, vm.Config{})
			txStart := time.Now()
			for _, msg := range msgs {
				res, err := core.ApplyMessage(evm, msg, new(core.GasPool).AddGas(msg.GasLimit))
				intervalTxCount++
				if err == nil && !res.Failed() {
					intervalSuccessTxCount++
				}
			}
			txDuration := time.Since(txStart)
			totalTxTime += txDuration
			if txDuration > maxTxTime {
				maxTxTime = txDuration
			}

			if (cfg.UseBinaryTrie || !cfg.UseVerkle) && cfg.PruneInterval > 0 && b%uint64(cfg.PruneInterval) == 0 {
				pruneStart := time.Now()
				statedb.PruneNextShard()
				pruneDuration := time.Since(pruneStart)
				totalPruneTime += pruneDuration
				if pruneDuration > maxPruneTime {
					maxPruneTime = pruneDuration
				}
				pruneCount++
			}

			// 2. State root calculation time statistics
			rootStart := time.Now()
			statedb.Finalise(false)
			finaliseDuration := time.Since(rootStart)

			commitStart := time.Now()
			h, _ := statedb.Commit(b, false, false)
			commitDuration := time.Since(commitStart)

			rootDuration := time.Since(rootStart)

			if b%100000 == 0 {
				fmt.Printf("[测试] 区块 %d: 最终处理周期: %v, 树根计算: %v, 提交耗时: %v, 总计: %v\n",
					b, finaliseDuration, commitDuration, rootDuration, rootDuration+finaliseDuration+commitDuration)
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
				if blockProofSize <= maxBlockSize || atomic.CompareAndSwapInt64(&common.BinaryBlockProofSizeMax, maxBlockSize, blockProofSize) {
					break
				}
			}

			// Optional treeDB commit
			if b%1000 == 0 {
				host.trieDB.Commit(h, false)
			}

			totalRootTime += rootDuration
			if rootDuration > maxRootTime {
				maxRootTime = rootDuration
			}

			intervalBlocks++
			totalProcessedBlocks++
			currentBlock++
			if intervalBlocks >= statsIv {
				reportStats()
			}
			// 每 10w 区块刷新一次假阳性分布
			if (b+1)%100000 == 0 {
				flushGlobalFPDistribution()
			}
		}
	}
	// 最后不足一个周期的统计报告
	if intervalBlocks > 0 {
		reportStats()
	}
	t.Logf("最终状态根: %s", lastStateRoot.String())
	flushGlobalFPDistribution() // 结束后强制刷新一次
}

func flushGlobalFPDistribution() {
	common.BinaryStatsMu.Lock()
	if len(common.BinaryFPDistribution) == 0 {
		common.BinaryStatsMu.Unlock()
		return
	}
	dist := make([]int64, len(common.BinaryFPDistribution))
	copy(dist, common.BinaryFPDistribution)
	common.BinaryFPDistribution = nil
	common.BinaryStatsMu.Unlock()

	f, err := os.OpenFile("global_fp_distribution.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	// 检查文件是否为空，写入表头
	if info, err := f.Stat(); err == nil && info.Size() == 0 {
		writer.Write([]string{"Bucket_Size"})
	}

	for _, d := range dist {
		writer.Write([]string{strconv.FormatInt(d, 10)})
	}
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
			if t.blockNum >= 54340 && t.blockNum <= 54345 {
				fmt.Printf("[%s] Block %d BalanceChange: addr=%s, reason=%d\n", t.prefix, t.blockNum, addr.Hex(), reason)
			}
		},
		OnNonceChangeV2: func(addr common.Address, prev, new uint64, reason tracing.NonceChangeReason) {
			t.count++
			t.hasher.Write(addr[:])
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], new)
			t.hasher.Write(b[:])
			if common.DebugFlag {
				fmt.Printf("[Tracer] NonceChange: addr=%s, new=%d, reason=%d\n", addr.Hex(), new, reason)
			}
		},
		OnCodeChange: func(addr common.Address, prevCodeHash common.Hash, prevCode []byte, codeHash common.Hash, code []byte) {
			t.count++
			t.hasher.Write(addr[:])
			t.hasher.Write(codeHash[:])
			if common.DebugFlag {
				fmt.Printf("[Tracer] CodeChange: addr=%s, new=%s\n", addr.Hex(), codeHash.Hex())
			}
		},
		OnStorageChange: func(addr common.Address, slot common.Hash, prev, new common.Hash) {
			t.count++
			t.hasher.Write(addr[:])
			t.hasher.Write(slot[:])
			t.hasher.Write(new[:])
			targetAddr := common.HexToAddress("0x59622442B567187157b85d6928A6c56e1E0841CA")
			targetSlot := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
			if (addr == targetAddr && slot == targetSlot) || (t.blockNum >= 54340 && t.blockNum <= 54345) {
				fmt.Printf("[%s] Block %d StorageChange: addr=%s, slot=%s, prev=%s, new=%s\n", t.prefix, t.blockNum, addr.Hex(), slot.Hex(), prev.Hex(), new.Hex())
			}
		},
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
		DbDir:            filepath.Join(os.TempDir(), "bin_consistency_db"),
		DataDir:          *dataDir,
		StartFileIdx:     *startIdx,
		EndFileIdx:       *endIdx,
		UseVerkle:        false,
		UseBinaryTrie:    true,
		UseMemory:        false,
		BinaryArchiveDir: filepath.Join(os.TempDir(), "bin_consistency_archive"),
		StartNum:         46147,
		PruneInterval:    0,
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

	mptLastRoot := types.EmptyRootHash
	binLastRoot := common.Hash{}

	mptTracer := newConsistencyTracer("MPT")
	binTracer := newConsistencyTracer("BIN")

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
				b := currentBlock
				miner := compareBlockMiners[b]
				reward, _ := uint256.FromBig(new(big.Int).Mul(big.NewInt(1e6), big.NewInt(1e18)))

				// MPT
				mptTracer.Reset()
				mptTracer.blockNum = b
				mptHost.trieDB.UpdateBlockNum(b)
				mptStateDB, _ := state.New(mptLastRoot, mptHost.sdb)
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
				binStateDB, _ := state.New(binLastRoot, binHost.sdb)
				binHooked := state.NewHookedState(binStateDB, binTracer.Hooks())
				if miner != (common.Address{}) {
					binHooked.AddBalance(miner, reward, tracing.BalanceChangeUnspecified)
				}
				if binCfg.PruneInterval > 0 && b%uint64(binCfg.PruneInterval) == 0 {
					binStateDB.PruneNextShard()
				}
				binHooked.Finalise(false)
				binRoot, _ := binStateDB.Commit(b, false, false)

				mptLastRoot = mptRoot
				binLastRoot = binRoot
				if b%1000 == 0 {
					mptHost.trieDB.Commit(mptRoot, false)
					binHost.trieDB.Commit(binRoot, false)
				}
				currentBlock++
			}

			// Process the block with transactions
			b := targetBlock
			msgs, _ := ts.PopBlock(b)
			miner := compareBlockMiners[b]
			reward, _ := uint256.FromBig(new(big.Int).Mul(big.NewInt(1e6), big.NewInt(1e18)))

			// A. Process with MPT
			mptTracer.Reset()
			mptTracer.blockNum = b
			mptHost.trieDB.UpdateBlockNum(b) // Changed from mptHost.sdb.SetBlockNum(b)
			mptStateDB, _ := state.New(mptLastRoot, mptHost.sdb)
			mptHooked := state.NewHookedState(mptStateDB, mptTracer.Hooks())

			if miner != (common.Address{}) {
				mptHooked.AddBalance(miner, reward, tracing.BalanceChangeUnspecified)
			}
			blockCtx := vm.BlockContext{
				CanTransfer: core.CanTransfer,
				Transfer:    core.Transfer,
				GetHash:     func(n uint64) common.Hash { return common.Hash{} },
				Coinbase:    miner,
				BlockNumber: new(big.Int).SetUint64(b),
				Time:        b * 15,
				Difficulty:  big.NewInt(1),
				BaseFee:     big.NewInt(0),
			}
			mptEVM := vm.NewEVM(blockCtx, mptHooked, params.MainnetChainConfig, vm.Config{})
			for _, msg := range msgs {
				core.ApplyMessage(mptEVM, msg, new(core.GasPool).AddGas(msg.GasLimit))
			}
			mptHooked.Finalise(false)
			mptRoot, _ := mptStateDB.Commit(b, false, false)
			mptSummary := mptTracer.Summary(b, mptRoot)

			// B. Process with Binary Trie
			binTracer.Reset()
			binTracer.blockNum = b
			binHost.sdb.SetBlockNum(b)
			binStateDB, _ := state.New(binLastRoot, binHost.sdb)
			binHooked := state.NewHookedState(binStateDB, binTracer.Hooks())

			if miner != (common.Address{}) {
				binHooked.AddBalance(miner, reward, tracing.BalanceChangeUnspecified)
			}
			binEVM := vm.NewEVM(blockCtx, binHooked, params.MainnetChainConfig, vm.Config{})
			for _, msg := range msgs {
				core.ApplyMessage(binEVM, msg, new(core.GasPool).AddGas(msg.GasLimit))
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

			// C. Compare
			if mptSummary.WriteCount != binSummary.WriteCount {
				t.Fatalf("Block %d: WriteCount mismatch! MPT=%d, BIN=%d", b, mptSummary.WriteCount, binSummary.WriteCount)
			}
			if mptSummary.WriteHash != binSummary.WriteHash {
				t.Fatalf("Block %d: WriteHash mismatch! MPT=%s, BIN=%s (WriteCount=%d), binRoot=%s", b, mptSummary.WriteHash.Hex(), binSummary.WriteHash.Hex(), mptSummary.WriteCount, binRoot.Hex())
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
			currentBlock++
		}
	}
}
