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

// CacheProcessorHost encapsulates the environment for state processing experiments.
type CacheProcessorHost struct {
	db        ethdb.Database
	trieDB    *triedb.Database
	sdb       *state.CachingDB
	snaps     *snapshot.Tree
	config    *CacheProcessorConfig
	preTrieDB *triedb.Database
	preSdb    *state.CachingDB
}

var (
	dbDirCache            = flag.String("dbDirCache", "F:\\ethdata\\expire_state_db", "Database directory")
	dataDirCache          = flag.String("dataDirCache", "E:\\ethdata", "Input data directory")
	startIdxCache         = flag.Int("startFileIdxCache", 1, "Start file index")
	endIdxCache           = flag.Int("endFileIdxCache", 21, "End file index")
	useVerkleCache        = flag.Bool("useVerkleCache", false, "Enable Verkle trie")
	useBinaryTrieCache    = flag.Bool("useBinaryTrieCache", false, "Enable Binary trie")
	useCacheTrieCache     = flag.Bool("useCacheTrieCache", true, "Enable CacheTrie")
	useMemoryCache        = flag.Bool("useMemoryCache", false, "Use in-memory DB")
	binaryArchiveDirCache = flag.String("binaryArchiveDirCache", "", "Binary trie archive directory")
	statsIntervalCache    = flag.Int("statsIntervalCache", 100000, "Statistics reporting interval (in blocks)")
)

type CacheProcessorConfig struct {
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
}

func NewCacheProcessorHost(cfg *CacheProcessorConfig) (*CacheProcessorHost, error) {
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

	//trieDB := triedb.NewDatabase(db, &triedb.Config{
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

	host := &CacheProcessorHost{
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

func (h *CacheProcessorHost) Close() {
	if h.db != nil {
		h.db.Close()
	}
}

// CommitToPreTrie handles the asynchronous commitment of CacheTrie data to the underlying trie.
func (h *CacheProcessorHost) CommitToPreTrie(root common.Hash, blockNum uint64, deleteKVList *cachetrie.DeleteKVList) {
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
		cleanStateDB, _ := state.New(newRoot, h.preSdb)

		// Process accounts
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

		commitRoot, _ := cleanStateDB.Commit(blockNum, false, false)
		newRoot = commitRoot
		h.preTrieDB.Commit(newRoot, false)
	}

	h.trieDB.CacheTrie().FinishCleanup(blockNum, newRoot)
	runtime.GC()
}

// Transaction loading is now handled by TransactionStreamer in processor_utils.go

func TestCacheStateProcessor(t *testing.T) {
	fmt.Println(">>> Starting TestCacheStateProcessor")
	if !flag.Parsed() {
		flag.Parse()
	}
	cfg := &CacheProcessorConfig{
		DbDir:            *dbDirCache,
		DataDir:          *dataDirCache,
		StartFileIdx:     *startIdxCache,
		EndFileIdx:       *endIdxCache,
		UseVerkle:        *useVerkleCache,
		UseBinaryTrie:    *useBinaryTrieCache,
		UseCacheTrie:     *useCacheTrieCache,
		UseMemory:        *useMemoryCache,
		BinaryArchiveDir: *binaryArchiveDirCache,
		StartNum:         46147,
	}

	common.UseVerkle = cfg.UseVerkle
	common.UseCacheTrie = cfg.UseCacheTrie
	if cfg.UseVerkle && cfg.UseCacheTrie {
		common.VerkleLayerCount = 128
	}

	host, err := NewCacheProcessorHost(cfg)
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer host.Close()
	state.ResetCacheStats() // 从零开始统计

	files, err := compareFindTransactionFiles(cfg.DataDir)
	if err != nil || len(files) == 0 {
		t.Fatalf("failed to find transaction files: %v", err)
	}

	selectedFiles := files[cfg.StartFileIdx-1 : cfg.EndFileIdx]
	fmt.Printf(">>> Found %d transaction files, selected indices %d to %d (count: %d)\n", len(files), cfg.StartFileIdx, cfg.EndFileIdx, len(selectedFiles))
	// Create genesis block and blockchain
	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{},
	}
	gspec.MustCommit(host.db, host.trieDB)
	lastStateRoot := types.EmptyRootHash
	fmt.Printf(">>> Genesis block committed, root: %s\n", lastStateRoot.Hex())

	// Statistics tracking
	statsIv := uint64(*statsIntervalCache)
	var (
		intervalBlocks uint64
		totalTxTime    time.Duration
		maxTxTime      time.Duration
		totalRootTime  time.Duration
		maxRootTime    time.Duration

		// CacheTrie stats
		lastAcctHit, lastAcctMissEx, lastAcctMissNo                        int64
		lastStorHit, lastStorMissEx, lastStorMissNo                        int64
		lastTotAcctReads, lastTotStorReads, lastTotAcctUpd, lastTotStorUpd int64
		lastTotReads, lastTotUpdates                                       int64
		totalProcessedBlocks                                               uint64
		intervalTxCount                                                    uint64
		intervalSuccessTxCount                                             uint64
	)

	// CSV file setup
	csvFile, err := os.Create("cache_stats_cache_only.csv")
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

	reportStats := func() {
		if intervalBlocks == 0 {
			return
		}
		fmt.Printf("Blocks: %d - %d (Processed Blocks Count)\n", totalProcessedBlocks-intervalBlocks, totalProcessedBlocks-1)
		fmt.Printf("  Tx Execution   - Avg: %v, Max: %v\n", totalTxTime/time.Duration(intervalBlocks), maxTxTime)
		if intervalTxCount > 0 {
			fmt.Printf("  Tx Success Rate - %.2f%% (%d/%d)\n", float64(intervalSuccessTxCount)*100/float64(intervalTxCount), intervalSuccessTxCount, intervalTxCount)
		}
		fmt.Printf("  Root Calculate - Avg: %v, Max: %v\n", totalRootTime/time.Duration(intervalBlocks), maxRootTime)

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

		fmt.Printf("  [5 Metrics Summary]:\n")
		fmt.Printf("    1. 读取次数 (Total Reads): %d\n", totalReads-lastTotReads)
		fmt.Printf("    2. 写入次数 (Total Writes): %d\n", totalUpdates-lastTotUpdates)
		fmt.Printf("    3. 读取不中的次数 (MissNo): %d\n", (acctMissNo+storMissNo)-(lastAcctMissNo+lastStorMissNo))
		fmt.Printf("    4. 读取中了在缓存 (CacheHit): %d\n", (acctHit+storHit)-(lastAcctHit+lastStorHit))
		fmt.Printf("    5. 读取中了不在缓存 (MissEx): %d\n", (acctMissEx+storMissEx)-(lastAcctMissEx+lastStorMissEx))

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
			strconv.FormatUint(totalProcessedBlocks-intervalBlocks, 10),
			strconv.FormatUint(totalProcessedBlocks-1, 10),
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
		intervalTxCount = 0
		intervalSuccessTxCount = 0

		lastAcctHit, lastAcctMissEx, lastAcctMissNo = acctHit, acctMissEx, acctMissNo
		lastStorHit, lastStorMissEx, lastStorMissNo = storHit, storMissEx, storMissNo
		lastTotAcctReads, lastTotStorReads = totAcctReads, totStorReads
		lastTotAcctUpd, lastTotStorUpd = totAcctUpd, totStorUpd
		lastTotReads, lastTotUpdates = totalReads, totalUpdates
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

		// Process blocks sequentially from the streamer
		currentBlock, ok := ts.PeekBlockNum()
		if !ok {
			continue // skip empty file
		}

		start10k := time.Now()
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

				// Empty block root calculation
				if cfg.UseCacheTrie {
					_, resultHash, _ := statedb.PreCommit(false)
					root := lastStateRoot
					if resultHash != (common.Hash{}) {
						root = resultHash
					}
					// Handle code commitment
					codes := host.trieDB.CacheTrie().PopCodes()
					if len(codes) > 0 {
						batch := host.db.NewBatch()
						for codeHash, code := range codes {
							rawdb.WriteCode(batch, codeHash, code)
						}
						batch.Write()
					}
					deleteKVList := statedb.GetCachedDeleteKVList()
					if cfg.UseVerkle || cfg.UseBinaryTrie {
						host.CommitToPreTrie(root, b, deleteKVList)
					} else {
						go host.CommitToPreTrie(root, b, deleteKVList)
					}
					lastStateRoot = root
				} else {
					statedb.PreCommit(false)
					root, _ := statedb.PostCommit(b, false, false)
					lastStateRoot = root
					if b%100 == 0 {
						host.trieDB.Commit(root, false)
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
			b := targetBlock
			msgs, _ := ts.PopBlock(b)

			header := &types.Header{
				Number:     new(big.Int).SetUint64(b),
				GasLimit:   30000000,
				Time:       b * 15,
				Difficulty: big.NewInt(1),
			}

			host.trieDB.UpdateBlockNum(b)
			if host.trieDB.CacheTrie() != nil {
				host.trieDB.CacheTrie().SetBlockNum(b)
			}
			statedb, err := state.New(lastStateRoot, host.sdb)
			if err != nil {
				t.Fatalf("failed to create statedb at block %d with root %s: %v", b, lastStateRoot.Hex(), err)
			}

			// 0. Reward miner/packer
			miner := compareBlockMiners[b]

			// 1. Transaction execution time statistics
			txStart := time.Now()

			blockCtx := vm.BlockContext{
				CanTransfer: core.CanTransfer,
				Transfer:    core.Transfer,
				GetHash:     func(n uint64) common.Hash { return common.Hash{} },
				Coinbase:    miner,
				BlockNumber: new(big.Int).SetUint64(b),
				Time:        compareBlockTimestamps[b],
				Difficulty:  header.Difficulty,
				GasLimit:    1000000000,
				BaseFee:     big.NewInt(0),
			}

			vmenv := vm.NewEVM(blockCtx, statedb, params.MainnetChainConfig, vm.Config{})
			gp := new(core.GasPool).AddGas(blockCtx.GasLimit)

			// Pre-allocate balance for all senders (matches eth_compare_test)
			bigBalance := new(big.Int).Mul(big.NewInt(1e15), big.NewInt(1e18)) // 1M ETH
			balance, _ := uint256.FromBig(bigBalance)
			for _, m := range msgs {
				statedb.SetBalance(m.From, balance, tracing.BalanceChangeUnspecified)
			}

			for _, m := range msgs {
				intervalTxCount++
				m.SkipNonceChecks = true
				_, err := core.ApplyMessage(vmenv, m, gp)
				if err == nil {
					// Successful if ApplyMessage returns err == nil (matches eth_compare_test criteria)
					intervalSuccessTxCount++
				} else {
					toStr := "contract-creation"
					if m.To != nil {
						toStr = m.To.Hex()
					}
					fmt.Printf("Transaction Reverted: block=%d, sender=%s, nonce=%d, to=%s, res.Err=%v\n", b, m.From.Hex(), m.Nonce, toStr, err)
				}
			}

			txDuration := time.Since(txStart)
			totalTxTime += txDuration
			if txDuration > maxTxTime {
				maxTxTime = txDuration
			}

			// 2. Root calculation time statistics
			rootStart := time.Now()

			if cfg.UseCacheTrie {
				_, resultHash, _ := statedb.PreCommit(false)
				root := lastStateRoot
				if resultHash != (common.Hash{}) {
					root = resultHash
				}

				// Handle code commitment
				codes := host.trieDB.CacheTrie().PopCodes()
				if len(codes) > 0 {
					batch := host.db.NewBatch()
					for codeHash, code := range codes {
						rawdb.WriteCode(batch, codeHash, code)
					}
					batch.Write()
				}

				deleteKVList := statedb.GetCachedDeleteKVList()
				if cfg.UseVerkle || cfg.UseBinaryTrie {
					host.CommitToPreTrie(root, b, deleteKVList)
				} else {
					go host.CommitToPreTrie(root, b, deleteKVList)
				}
				lastStateRoot = root
			} else {
				statedb.PreCommit(false)
				root, _ := statedb.PostCommit(b, false, false)
				lastStateRoot = root
				if b%100 == 0 {
					host.trieDB.Commit(root, false)
				}
			}

			rootDuration := time.Since(rootStart)
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
		}
	}
	// Final statistics report for the last partial interval
	if intervalBlocks > 0 {
		reportStats()
	}
	t.Logf("Final state root: %s", lastStateRoot.String())
}
