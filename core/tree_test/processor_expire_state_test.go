package tree

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/cachetrie"
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
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
)

// ProcessorHost encapsulates the environment for state processing experiments.
type ProcessorHost struct {
	db        ethdb.Database
	trieDB    *triedb.Database
	sdb       *state.CachingDB
	snaps     *snapshot.Tree
	config    *ProcessorConfig
	preTrieDB *triedb.Database
	preSdb    *state.CachingDB
}

var (
	dbDir            = flag.String("dbDir2", "F:\\expire_data\\expire_state_db", "Database directory")
	dataDir          = flag.String("dataDir2", "E:\\ethdata", "Input data directory")
	startIdx         = flag.Int("startFileIdx2", 1, "Start file index")
	endIdx           = flag.Int("endFileIdx2", 21, "End file index")
	useVerkle        = flag.Bool("useVerkle2", false, "Enable Verkle trie")
	useBinaryTrie    = flag.Bool("useBinaryTrie2", true, "Enable Binary trie")
	useCacheTrie     = flag.Bool("useCacheTrie2", false, "Enable CacheTrie")
	useMemory        = flag.Bool("useMemory2", false, "Use in-memory DB")
	binaryArchiveDir = flag.String("binaryArchiveDir2", "F:\\expire_data\\expire_state_db_achive", "Binary trie archive directory")
	statsInterval    = flag.Int("statsInterval2", 100000, "Statistics reporting interval (in blocks)")
	pruneInterval    = flag.Int("pruneInterval", 5, "Blocks between Trie.PruneNextShard() calls")
)

func TestMain(m *testing.M) {
	if !flag.Parsed() {
		flag.Parse()
	}
	os.Exit(m.Run())
}

type ProcessorConfig struct {
	DbDir            string
	DataDir          string
	StartFileIdx     int
	EndFileIdx       int
	UseVerkle        bool
	UseBinaryTrie    bool
	UseCacheTrie     bool
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
	if cfg.UseVerkle || cfg.UseBinaryTrie {
		hdb = nil
	} else {
		pdb = nil
	}

	trieDB := triedb.NewFixedDatabase(db, &triedb.Config{
		Preimages:        false,
		IsVerkle:         cfg.UseVerkle,
		IsBinary:         cfg.UseBinaryTrie,
		CacheTrie:        cfg.UseCacheTrie,
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
	snaps, _ := snapshot.New(snapshot.Config{CacheSize: 100}, db, trieDB, firstRootHash)
	sdb := state.NewDatabase(trieDB, snaps)

	host := &ProcessorHost{
		db:     db,
		trieDB: trieDB,
		sdb:    sdb,
		snaps:  snaps,
		config: cfg,
	}

	if cfg.UseCacheTrie {
		if !cfg.UseVerkle {
			host.preTrieDB = triedb.NewFixedDatabase(db, &triedb.Config{
				Preimages: false,
				IsVerkle:  false,
				IsBinary:  cfg.UseBinaryTrie,
				CacheTrie: false,
				ReadCache: false,
				StartNum:  cfg.StartNum,
				PathDB:    nil,
				HashDB:    hdb,
			})
		} else {
			host.preTrieDB = triedb.NewDatabase2(db, &triedb.Config{
				Preimages: false,
				IsVerkle:  true,
				CacheTrie: false,
				ReadCache: false,
				StartNum:  cfg.StartNum,
				PathDB:    pdb,
				HashDB:    nil,
			}, trieDB.GetBackend())
		}
		host.preSdb = state.NewDatabase(host.preTrieDB, snaps)
	}

	return host, nil
}

func (h *ProcessorHost) Close() {
	if h.db != nil {
		h.db.Close()
	}
}

// Statistics tracking for CommitToPreTrie
var (
	preTrieSetupTime    time.Duration
	preTrieUpdateTime   time.Duration
	preTrieCommitTime   time.Duration
	preTrieDBCommitTime time.Duration
	preTrieGCTime       time.Duration
)

// CommitToPreTrie handles the asynchronous commitment of CacheTrie data to the underlying trie.
func (h *ProcessorHost) CommitToPreTrie(root common.Hash, blockNum uint64, deleteKVList *cachetrie.DeleteKVList) {
	if deleteKVList == nil {
		return
	}
	if len(deleteKVList.Data) == 0 {
		h.trieDB.CacheTrie().FinishCleanup(blockNum, root)
		return
	}

	data := deleteKVList.Data
	length := len(data)
	chunkSize := 2000
	if length > 100000 {
		chunkSize = 10000
	}

	newRoot := root
	for i := 0; i < length; i += chunkSize {
		end := i + chunkSize
		if end > length {
			end = length
		}
		chunk := data[i:end]

		t0 := time.Now()
		cleanStateDB, _ := state.New(newRoot, h.preSdb)
		preTrieSetupTime += time.Since(t0)

		// Process accounts
		t1 := time.Now()
		for _, kv := range chunk {
			if kv.Address == (common.Address{}) && len(kv.Key) > 0 {
				addr := common.BytesToAddress(kv.Key)
				cleanStateDB.SetAccount(addr, kv.Value, 0)
			}
		}

		// Process storage slots
		for _, kv := range chunk {
			if kv.Address != (common.Address{}) && len(kv.Key) > 0 {
				addr := kv.Address
				key := common.BytesToHash(kv.Key)
				if common.BytesToHash(kv.Value) == (common.Hash{}) {
					cleanStateDB.SetState(addr, key, common.Hash{})
				} else {
					_, vc, _, _ := rlp.Split(kv.Value)
					cleanStateDB.SetState(addr, key, common.BytesToHash(vc))
				}
			}
		}
		preTrieUpdateTime += time.Since(t1)

		t2 := time.Now()
		commitRoot, _ := cleanStateDB.Commit(blockNum, false, false)
		preTrieCommitTime += time.Since(t2)

		newRoot = commitRoot

		t3 := time.Now()
		h.preTrieDB.Commit(newRoot, false)
		preTrieDBCommitTime += time.Since(t3)
	}

	h.trieDB.CacheTrie().FinishCleanup(blockNum, newRoot)

	t4 := time.Now()
	runtime.GC()
	preTrieGCTime += time.Since(t4)
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
		UseCacheTrie:     *useCacheTrie,
		UseMemory:        *useMemory,
		BinaryArchiveDir: *binaryArchiveDir,
		StartNum:         46147,
		PruneInterval:    *pruneInterval,
	}

	common.UseVerkle = cfg.UseVerkle
	common.UseCacheTrie = cfg.UseCacheTrie
	if cfg.UseVerkle && cfg.UseCacheTrie {
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
		fmt.Printf("[Test] Failed to find transaction files: %v (len=%d)\n", err, len(files))
		t.Fatalf("failed to find transaction files: %v", err)
	}
	fmt.Printf("[Test] Found %d transaction files\n", len(files))

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

		totalTxTime    time.Duration
		maxTxTime      time.Duration
		totalRootTime  time.Duration
		maxRootTime    time.Duration
		totalPruneTime time.Duration

		// CacheTrie stats
		lastAcctHit, lastAcctMissEx, lastAcctMissNo                        int64
		lastStorHit, lastStorMissEx, lastStorMissNo                        int64
		lastTotAcctReads, lastTotStorReads, lastTotAcctUpd, lastTotStorUpd int64
		lastTotReads, lastTotUpdates                                       int64
		totalProcessedBlocks                                               uint64
	)

	// CSV file setup
	csvFile, err := os.Create("cache_stats_detailed.csv")
	if err != nil {
		t.Fatalf("failed to create csv file: %v", err)
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write CSV Header
	writer.Write([]string{
		"StartBlock", "EndBlock",
		"TotReads", "TotWrites", "TotMissNo", "TotCacheHit", "TotMissEx",
		"AcctReads", "AcctWrites", "AcctMissNo", "AcctCacheHit", "AcctMissEx",
		"StorReads", "StorWrites", "StorMissNo", "StorCacheHit", "StorMissEx",
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

			if b%10000 == 0 {
				fmt.Printf("[Test] Processing block %d (Total Processed: %d) duration: %v...\n", b, totalProcessedBlocks, time.Since(start10k))
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

			// 1. Transaction execution time statistics
			txStart := time.Now()

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
				applyStart := time.Now()
				core.ApplyMessage(evm, msg, new(core.GasPool).AddGas(msg.GasLimit))
				totalTxTime += time.Since(applyStart)
			}
			txDuration := time.Since(txStart)

			if txDuration > maxTxTime {
				maxTxTime = txDuration
			}

			// Prune Next Shard
			pruneStart := time.Now()
			if cfg.UseBinaryTrie && cfg.PruneInterval > 0 && b%uint64(cfg.PruneInterval) == 0 {
				statedb.PruneNextShard()
			}
			pruneDuration := time.Since(pruneStart)
			totalPruneTime += pruneDuration

			// 2. State root calculation time statistics
			rootStart := time.Now()
			statedb.Finalise(false)
			finaliseDuration := time.Since(rootStart)

			commitStart := time.Now()
			h, _ := statedb.Commit(b, false, false)
			commitDuration := time.Since(commitStart)

			rootDuration := time.Since(rootStart)

			if b%10000 == 0 {
				fmt.Printf("[Test] Block %d: TxExec: %v, Prune: %v, Finalise: %v, Commit: %v, RootCalc: %v, Total: %v\n",
					b, txDuration, pruneDuration, finaliseDuration, commitDuration, rootDuration, txDuration+rootDuration)
			}
			lastStateRoot = h

			// Optional treeDB commit
			if b%100 == 0 {
				host.trieDB.Commit(h, false)
			}

			totalRootTime += rootDuration
			if rootDuration > maxRootTime {
				maxRootTime = rootDuration
			}

			intervalBlocks++
			totalProcessedBlocks++
			if b > 0 && (b+1)%statsIv == 0 {
				fmt.Printf("Blocks: %d - %d\n", intervalStartBlock, b)
				fmt.Printf("  Tx Execution   - Avg: %v, Max: %v\n", totalTxTime/time.Duration(intervalBlocks), maxTxTime)
				fmt.Printf("  Root Calculate - Avg: %v, Max: %v\n", totalRootTime/time.Duration(intervalBlocks), maxRootTime)
				fmt.Printf("  PruneNextShard - Avg: %v\n", totalPruneTime/time.Duration(intervalBlocks))
				fmt.Printf("  CommitToPreTrie - Setup: %v, Update: %v, Commit: %v, DBCommit: %v, GC: %v\n",
					preTrieSetupTime, preTrieUpdateTime, preTrieCommitTime, preTrieDBCommitTime, preTrieGCTime)

				// Reset sub-timers
				preTrieSetupTime, preTrieUpdateTime, preTrieCommitTime, preTrieDBCommitTime, preTrieGCTime = 0, 0, 0, 0, 0

				// CacheTrie stats
				acctHit, acctMissEx, acctMissNo, storHit, storMissEx, storMissNo, _, _, _, _,
					totAcctReads, totStorReads, totAcctUpd, totStorUpd,
					totalReads, totalUpdates := state.GetCacheStats()

				deltaAcctHit := acctHit - lastAcctHit
				deltaAcctMissEx := acctMissEx - lastAcctMissEx
				deltaAcctMissNo := acctMissNo - lastAcctMissNo
				totalAcct := deltaAcctHit + deltaAcctMissEx + deltaAcctMissNo

				deltaStorHit := storHit - lastStorHit
				deltaStorMissEx := storMissEx - lastStorMissEx
				deltaStorMissNo := storMissNo - lastStorMissNo
				totalStor := deltaStorHit + deltaStorMissEx + deltaStorMissNo

				if cfg.UseCacheTrie {
					fmt.Printf("  [5 Metrics Summary]:\n")
					fmt.Printf("    1. 读取次数 (Total Reads): %d\n", totalReads-lastTotReads)
					fmt.Printf("    2. 写入次数 (Total Writes): %d\n", totalUpdates-lastTotUpdates)
					fmt.Printf("    3. 读取不中的次数 (MissNo): %d\n", (acctMissNo+storMissNo)-(lastAcctMissNo+lastStorMissNo))
					fmt.Printf("    4. 读取中了在缓存 (CacheHit): %d\n", (acctHit+storHit)-(lastAcctHit+lastStorHit))
					fmt.Printf("    5. 读取中了不在缓存 (MissEx): %d\n", (acctMissEx+storMissEx)-(lastAcctMissEx+lastStorMissEx))
				}
				if totalAcct > 0 {
					fmt.Printf("  Cache Account  - Hit: %d (%.2f%%), MissEx: %d (%.2f%%), MissNo: %d (%.2f%%), TotRead: %d, TotUpd: %d\n",
						deltaAcctHit, float64(deltaAcctHit)*100/float64(totalAcct),
						deltaAcctMissEx, float64(deltaAcctMissEx)*100/float64(totalAcct),
						deltaAcctMissNo, float64(deltaAcctMissNo)*100/float64(totalAcct),
						totAcctReads-lastTotAcctReads, totAcctUpd-lastTotAcctUpd)
				}
				if totalStor > 0 {
					fmt.Printf("  Cache Storage  - Hit: %d (%.2f%%), MissEx: %d (%.2f%%), MissNo: %d (%.2f%%), TotRead: %d, TotUpd: %d\n",
						deltaStorHit, float64(deltaStorHit)*100/float64(totalStor),
						deltaStorMissEx, float64(deltaStorMissEx)*100/float64(totalStor),
						deltaStorMissNo, float64(deltaStorMissNo)*100/float64(totalStor),
						totStorReads-lastTotStorReads, totStorUpd-lastTotStorUpd)
				}

				// Write to CSV
				record := []string{
					strconv.FormatUint(intervalStartBlock, 10),
					strconv.FormatUint(b, 10),
					// Aggregated
					strconv.FormatInt(totalReads-lastTotReads, 10),
					strconv.FormatInt(totalUpdates-lastTotUpdates, 10),
					strconv.FormatInt((acctMissNo+storMissNo)-(lastAcctMissNo+lastStorMissNo), 10),
					strconv.FormatInt((acctHit+storHit)-(lastAcctHit+lastStorHit), 10),
					strconv.FormatInt((acctMissEx+storMissEx)-(lastAcctMissEx+lastStorMissEx), 10),
					// Account specific
					strconv.FormatInt(totAcctReads-lastTotAcctReads, 10),
					strconv.FormatInt(totAcctUpd-lastTotAcctUpd, 10),
					strconv.FormatInt(acctMissNo-lastAcctMissNo, 10),
					strconv.FormatInt(acctHit-lastAcctHit, 10),
					strconv.FormatInt(acctMissEx-lastAcctMissEx, 10),
					// Storage specific
					strconv.FormatInt(totStorReads-lastTotStorReads, 10),
					strconv.FormatInt(totStorUpd-lastTotStorUpd, 10),
					strconv.FormatInt(storMissNo-lastStorMissNo, 10),
					strconv.FormatInt(storHit-lastStorHit, 10),
					strconv.FormatInt(storMissEx-lastStorMissEx, 10),
				}
				writer.Write(record)
				writer.Flush()

				// Reset stats
				intervalBlocks = 0
				totalTxTime = 0
				maxTxTime = 0
				totalRootTime = 0
				maxRootTime = 0
				totalPruneTime = 0

				lastAcctHit, lastAcctMissEx, lastAcctMissNo = acctHit, acctMissEx, acctMissNo
				lastStorHit, lastStorMissEx, lastStorMissNo = storHit, storMissEx, storMissNo
				lastTotAcctReads, lastTotStorReads = totAcctReads, totStorReads
				lastTotAcctUpd, lastTotStorUpd = totAcctUpd, totStorUpd
				lastTotReads, lastTotUpdates = totalReads, totalUpdates

				intervalStartBlock = b + 1
			}
		}
	}
	// Final statistics report for the last partial interval
	if intervalBlocks > 0 {
		fmt.Printf("Final Partial Interval (Blocks: %d - %d)\n", intervalStartBlock, lastProcessedBlock)
		fmt.Printf("  Tx Execution   - Avg: %v, Max: %v\n", totalTxTime/time.Duration(intervalBlocks), maxTxTime)
		fmt.Printf("  Root Calculate - Avg: %v, Max: %v\n", totalRootTime/time.Duration(intervalBlocks), maxRootTime)
		fmt.Printf("  PruneNextShard - Avg: %v\n", totalPruneTime/time.Duration(intervalBlocks))

		// Final CacheTrie stats
		acctHit, acctMissEx, acctMissNo, storHit, storMissEx, storMissNo, _, _, _, _,
			totAcctReads, totStorReads, totAcctUpd, totStorUpd,
			totalReads, totalUpdates := state.GetCacheStats()

		fmt.Printf("  [5 Metrics Summary]:\n")
		fmt.Printf("    1. 读取次数 (Total Reads): %d\n", totalReads-lastTotReads)
		fmt.Printf("    2. 写入次数 (Total Writes): %d\n", totalUpdates-lastTotUpdates)
		fmt.Printf("    3. 读取不中的次数 (MissNo): %d\n", (acctMissNo+storMissNo)-(lastAcctMissNo+lastStorMissNo))
		fmt.Printf("    4. 读取中了在缓存 (CacheHit): %d\n", (acctHit+storHit)-(lastAcctHit+lastStorHit))
		fmt.Printf("    5. 读取中了不在缓存 (MissEx): %d\n", (acctMissEx+storMissEx)-(lastAcctMissEx+lastStorMissEx))
		deltaAcctHit := acctHit - lastAcctHit
		deltaAcctMissEx := acctMissEx - lastAcctMissEx
		deltaAcctMissNo := acctMissNo - lastAcctMissNo
		totalAcct := deltaAcctHit + deltaAcctMissEx + deltaAcctMissNo

		deltaStorHit := storHit - lastStorHit
		deltaStorMissEx := storMissEx - lastStorMissEx
		deltaStorMissNo := storMissNo - lastStorMissNo
		totalStor := deltaStorHit + deltaStorMissEx + deltaStorMissNo

		if totalAcct > 0 {
			fmt.Printf("  Cache Account  - Hit: %d (%.2f%%), MissEx: %d (%.2f%%), MissNo: %d (%.2f%%), TotRead: %d, TotUpd: %d\n",
				deltaAcctHit, float64(deltaAcctHit)*100/float64(totalAcct),
				deltaAcctMissEx, float64(deltaAcctMissEx)*100/float64(totalAcct),
				deltaAcctMissNo, float64(deltaAcctMissNo)*100/float64(totalAcct),
				totAcctReads-lastTotAcctReads, totAcctUpd-lastTotAcctUpd)
		}
		if totalStor > 0 {
			fmt.Printf("  Cache Storage  - Hit: %d (%.2f%%), MissEx: %d (%.2f%%), MissNo: %d (%.2f%%), TotRead: %d, TotUpd: %d\n",
				deltaStorHit, float64(deltaStorHit)*100/float64(totalStor),
				deltaStorMissEx, float64(deltaStorMissEx)*100/float64(totalStor),
				deltaStorMissNo, float64(deltaStorMissNo)*100/float64(totalStor),
				totStorReads-lastTotStorReads, totStorUpd-lastTotStorUpd)
		}

		// Final Write to CSV
		record := []string{
			strconv.FormatUint(intervalStartBlock, 10),
			strconv.FormatUint(lastProcessedBlock, 10),
			// Aggregated
			strconv.FormatInt(totalReads-lastTotReads, 10),
			strconv.FormatInt(totalUpdates-lastTotUpdates, 10),
			strconv.FormatInt((acctMissNo+storMissNo)-(lastAcctMissNo+lastStorMissNo), 10),
			strconv.FormatInt((acctHit+storHit)-(lastAcctHit+lastStorHit), 10),
			strconv.FormatInt((acctMissEx+storMissEx)-(lastAcctMissEx+lastStorMissEx), 10),
			// Account specific
			strconv.FormatInt(totAcctReads-lastTotAcctReads, 10),
			strconv.FormatInt(totAcctUpd-lastTotAcctUpd, 10),
			strconv.FormatInt(acctMissNo-lastAcctMissNo, 10),
			strconv.FormatInt(acctHit-lastAcctHit, 10),
			strconv.FormatInt(acctMissEx-lastAcctMissEx, 10),
			// Storage specific
			strconv.FormatInt(totStorReads-lastTotStorReads, 10),
			strconv.FormatInt(totStorUpd-lastTotStorUpd, 10),
			strconv.FormatInt(storMissNo-lastStorMissNo, 10),
			strconv.FormatInt(storHit-lastStorHit, 10),
			strconv.FormatInt(storMissEx-lastStorMissEx, 10),
		}
		writer.Write(record)
		writer.Flush()
	}
	t.Logf("Final state root: %s", lastStateRoot.String())
}
