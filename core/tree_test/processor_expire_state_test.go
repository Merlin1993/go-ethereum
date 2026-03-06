package tree

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/big"
	"os"
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
	if cfg.UseVerkle || cfg.UseBinaryTrie || !cfg.UseBinaryTrie && !cfg.UseVerkle {
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

// LoadTransactionsFromCSV reads and parses transactions from a CSV file.
func LoadTransactionsFromCSV(file string) (map[uint64][]*core.Message, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	_, err = reader.Read() // skip header
	if err != nil {
		return nil, err
	}

	msgsByBlock := make(map[uint64][]*core.Message)
	count := 0
	start := time.Now()
	fmt.Printf("[Test] Loading transactions from %s...\n", file)
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		count++
		if count%1000000 == 0 {
			fmt.Printf("[Test] Loaded %d transactions so far...\n", count)
		}
		if len(record) < 10 || record[0] == "hash" {
			continue
		}

		blockNum, _ := strconv.ParseUint(record[3], 10, 64)
		from := common.HexToAddress(record[5])
		var to *common.Address
		if record[6] != "" && record[6] != "null" {
			toAddr := common.HexToAddress(record[6])
			to = &toAddr
		}

		value := new(big.Int)
		value.SetString(record[7], 10)

		gasLimit, _ := strconv.ParseUint(record[8], 10, 64)
		if gasLimit == 0 {
			gasLimit = 21000
		}

		gasPrice := new(big.Int)
		gasPrice.SetString(record[9], 10)
		if gasPrice.Sign() == 0 {
			gasPrice = big.NewInt(1000000000)
		}

		nonce, _ := strconv.ParseUint(record[1], 10, 64)
		data := common.FromHex(record[10])

		msg := &core.Message{
			To:               to,
			From:             from,
			Nonce:            nonce,
			Value:            value,
			GasLimit:         gasLimit,
			GasPrice:         gasPrice,
			GasFeeCap:        gasPrice,
			GasTipCap:        gasPrice,
			Data:             data,
			SkipNonceChecks:  true,
			SkipFromEOACheck: false,
		}
		msgsByBlock[blockNum] = append(msgsByBlock[blockNum], msg)
	}
	fmt.Printf("[Test] Loaded %d transactions total in %v\n", count, time.Since(start))
	return msgsByBlock, nil
}

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
		intervalBlocks     uint64
		intervalStartBlock uint64
		firstBlockSet      bool
		lastProcessedBlock uint64

		totalRootTime  time.Duration
		maxRootTime    time.Duration
		totalPruneTime time.Duration
		maxPruneTime   time.Duration
		pruneCount     uint64

		totalProcessedBlocks uint64
		epochID              uint64
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
		"Epoch_ID", "Cumulative_Storage_Bytes", "Avg_Root_Calc_Time_ms", "Max_Root_Calc_Time_ms", "Avg_Pruning_Time_us", "Max_Pruning_Time_us",
		"Hit_Count", "Miss_NonExistent_Count", "Miss_Existent_Count", "Cycle_FP_Count", "Max_FP_In_Single_Block",
	})

	for _, file := range selectedFiles {
		t.Logf("Processing file: %s", file)
		msgsByBlock, err := LoadTransactionsFromCSV(file)
		if err != nil {
			t.Errorf("failed to load transactions from %s: %v", file, err)
			continue
		}

		// Load block metadata (timestamps and miners) once per file
		fileIdx := compareGetFileIndex(file)
		compareLoadBlockTimestampsFromFile(cfg.DataDir, fileIdx)

		var minBlock, maxBlock uint64 = 1e18, 0
		for b := range msgsByBlock {
			if b < minBlock {
				minBlock = b
			}
			if b > maxBlock {
				maxBlock = b
			}
		}

		start10k := time.Now()
		for b := minBlock; b <= maxBlock; b++ {
			if !firstBlockSet {
				intervalStartBlock = b
				firstBlockSet = true
			}
			lastProcessedBlock = b

			if b%100000 == 0 {
				fmt.Printf("[测试] 正在处理区块 %d (总计已处理: %d) 耗时: %v...\n", b, totalProcessedBlocks, time.Since(start10k))
				start10k = time.Now()
			}
			msgs := msgsByBlock[b]
			host.sdb.SetBlockNum(b)
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
			for _, msg := range msgs {
				core.ApplyMessage(evm, msg, new(core.GasPool).AddGas(msg.GasLimit))
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

			// 统计单区块假阳性最大次数
			fpInBlock := atomic.SwapInt64(&common.BinaryTrieFPInBlock, 0)
			for {
				maxFP := atomic.LoadInt64(&common.BinaryMaxFPInSingleBlock)
				if fpInBlock <= maxFP || atomic.CompareAndSwapInt64(&common.BinaryMaxFPInSingleBlock, maxFP, fpInBlock) {
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
			if b > 0 && (b+1)%statsIv == 0 {
				epochID++
				storageSize, _ := getDirSize(cfg.DbDir)

				fmt.Printf("周期 %d (区块范围: %d - %d)\n", epochID, intervalStartBlock, b)
				fmt.Printf("  累计存储占用: %d 字节\n", storageSize)
				fmt.Printf("  平均根计算耗时: %.2f ms\n", float64(totalRootTime.Milliseconds())/float64(intervalBlocks))
				fmt.Printf("  最大根计算耗时: %v\n", maxRootTime)

				avgPruneTime := 0.0
				if pruneCount > 0 {
					avgPruneTime = float64(totalPruneTime.Microseconds()) / float64(pruneCount)
				}
				fmt.Printf("  平均裁剪耗时: %.2f us\n", avgPruneTime)
				fmt.Printf("  最大裁剪耗时: %v\n", maxPruneTime)
				fmt.Printf("  命中热状态次数: %d\n", atomic.LoadInt64(&common.BinaryHitCount))
				fmt.Printf("  未命中且数据不存在次数: %d\n", atomic.LoadInt64(&common.BinaryMissNonExistentCount))
				fmt.Printf("  未命中但数据存在次数: %d\n", atomic.LoadInt64(&common.BinaryMissExistentCount))
				fmt.Printf("  假阳性触发次数: %d\n", atomic.LoadInt64(&common.BinaryCycleFPCount))
				fmt.Printf("  单区块假阳性最大次数: %d\n", atomic.LoadInt64(&common.BinaryMaxFPInSingleBlock))

				// 写入 CSV
				record := []string{
					strconv.FormatUint(epochID, 10),
					strconv.FormatInt(storageSize, 10),
					fmt.Sprintf("%.2f", float64(totalRootTime.Milliseconds())/float64(intervalBlocks)),
					strconv.FormatInt(maxRootTime.Milliseconds(), 10),
					fmt.Sprintf("%.2f", avgPruneTime),
					strconv.FormatInt(maxPruneTime.Microseconds(), 10),
					strconv.FormatInt(atomic.LoadInt64(&common.BinaryHitCount), 10),
					strconv.FormatInt(atomic.LoadInt64(&common.BinaryMissNonExistentCount), 10),
					strconv.FormatInt(atomic.LoadInt64(&common.BinaryMissExistentCount), 10),
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

				atomic.StoreInt64(&common.BinaryHitCount, 0)
				atomic.StoreInt64(&common.BinaryMissNonExistentCount, 0)
				atomic.StoreInt64(&common.BinaryMissExistentCount, 0)
				atomic.StoreInt64(&common.BinaryCycleFPCount, 0)
				atomic.StoreInt64(&common.BinaryMaxFPInSingleBlock, 0)

				intervalStartBlock = b + 1
			}
		}
	}
	// 最后不足一个周期的统计报告
	if intervalBlocks > 0 {
		epochID++
		storageSize, _ := getDirSize(cfg.DbDir)

		fmt.Printf("最终周期 %d (区块范围: %d - %d)\n", epochID, intervalStartBlock, lastProcessedBlock)
		fmt.Printf("  累计存储占用: %d 字节\n", storageSize)
		fmt.Printf("  平均根计算耗时: %.2f ms\n", float64(totalRootTime.Milliseconds())/float64(intervalBlocks))
		fmt.Printf("  最大根计算耗时: %v\n", maxRootTime)

		avgPruneTime := 0.0
		if pruneCount > 0 {
			avgPruneTime = float64(totalPruneTime.Microseconds()) / float64(pruneCount)
		}
		fmt.Printf("  平均裁剪耗时: %.2f us\n", avgPruneTime)
		fmt.Printf("  最大裁剪耗时: %v\n", maxPruneTime)

		// 写入 CSV
		record := []string{
			strconv.FormatUint(epochID, 10),
			strconv.FormatInt(storageSize, 10),
			fmt.Sprintf("%.2f", float64(totalRootTime.Milliseconds())/float64(intervalBlocks)),
			strconv.FormatInt(maxRootTime.Milliseconds(), 10),
			fmt.Sprintf("%.2f", avgPruneTime),
			strconv.FormatInt(maxPruneTime.Microseconds(), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryHitCount), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryMissNonExistentCount), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryMissExistentCount), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryCycleFPCount), 10),
			strconv.FormatInt(atomic.LoadInt64(&common.BinaryMaxFPInSingleBlock), 10),
		}
		writer.Write(record)
		writer.Flush()
	}
	t.Logf("最终状态根: %s", lastStateRoot.String())
}
