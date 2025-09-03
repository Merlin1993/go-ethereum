package tree

import (
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/triedb/hashdb"

	"github.com/ethereum/go-ethereum/cachetrie"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/triedb/pathdb"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"

	"encoding/csv"
)

// TestCompareProcessTransactions tests processing transactions from CSV
func TestCompareProcessTransactions(t *testing.T) {
	// Define database paths
	dbDir := "F:\\ethdata\\geth_compare_db_mpt3"
	statsDir := "F:\\ethdata\\compare_stats_6_mpt"
	dataDir := "E:\\ethdata"

	// Specify file range, hardcoded way to specify start and end file indices
	startFileIdx := 1 // Start file index (starting from 1)
	endFileIdx := 11  // End file index
	//46147
	var startNum uint64 = 46147

	// Create statistics aggregator
	statsAgg := NewCompareStatsAggregator(statsDir, 100000) // Use direct value instead of constant

	// Add: Create state tree statistics recorder
	trieStatsDir := filepath.Join(statsDir, "trie_stats")
	standardTrieRecorder := CreateTrieStatsRecorder(trieStatsDir, dbDir, StandardTrie)
	cacheTrieRecorder := CreateTrieStatsRecorder(trieStatsDir, dbDir, CacheTrie)
	verkleTrieRecorder := CreateTrieStatsRecorder(trieStatsDir, dbDir, VerkleTrie)

	// Create or open persistent database
	ldb, err := leveldb.New(dbDir, 1024, 1024, "eth-compare-process-test", false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer ldb.Close()

	db := rawdb.NewDatabase(ldb)
	hashdb := hashdb.Defaults
	pathdb := pathdb.Defaults
	if common.UserVerkle {
		hashdb = nil
	} else {
		pathdb = nil
	}
	trieDB := triedb.NewDatabase(db, &triedb.Config{
		Preimages: false,
		IsVerkle:  common.UserVerkle,
		CacheTrie: common.UseCacheTrie,
		ReadCache: false,
		StartNum:  startNum,
		PathDB:    pathdb,
		HashDB:    hashdb,
	})

	// Use correct state package API
	var snaps *snapshot.Tree
	firstRootHash := types.EmptyRootHash
	if common.UserVerkle {
		firstRootHash = common.Hash{}
	}
	snaps, _ = snapshot.New(snapshot.Config{CacheSize: 100}, db, trieDB, firstRootHash)
	sdb := state.NewDatabase(trieDB, snaps)
	var preTrieDB *triedb.Database
	var preSdb *state.CachingDB
	if common.UseCacheTrie {
		if !common.UserVerkle {
			preTrieDB = triedb.NewDatabase(db, &triedb.Config{
				Preimages: false,
				IsVerkle:  common.UserVerkle,
				CacheTrie: false,
				ReadCache: false,
				StartNum:  startNum,
				PathDB:    pathdb,
				HashDB:    hashdb,
			})
		} else {
			preTrieDB = triedb.NewDatabase2(db, &triedb.Config{
				Preimages: false,
				IsVerkle:  true,
				CacheTrie: false,
				ReadCache: false,
				StartNum:  startNum,
				PathDB:    pathdb,
				HashDB:    hashdb,
			}, trieDB.GetBackend())
		}
		preSdb = state.NewDatabase(preTrieDB, snaps)
	}

	// Create genesis block and blockchain
	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{},
	}
	genesis := gspec.MustCommit(db, trieDB)

	// Save last processed block and state root
	lastProcessedBlock := genesis
	var lastStateRoot common.Hash

	// Try to read last run state root from database, support checkpoint continuation
	lastRunRoot := rawdb.ReadLastRunStateRoot(db)
	if lastRunRoot != (common.Hash{}) {
		// Found last run state root, use it as starting point
		t.Logf("Found last run state root: %s, will continue processing from this state", lastRunRoot.String())
		lastStateRoot = lastRunRoot
	} else {
		// Use genesis block state root
		if genesis != nil {
			lastStateRoot = genesis.Root()
		}
		t.Logf("No last run state root found, using genesis block state root: %s", lastStateRoot.String())
	}

	// Create state access counter
	counter := NewCompareStateAccessCounter()

	// Find all transaction files
	files, err := compareFindTransactionFiles(dataDir)
	if err != nil {
		t.Fatalf("Failed to find CSV files: %v", err)
	}

	if len(files) == 0 {
		t.Fatalf("No transaction files found")
	}

	// Validate file range
	if startFileIdx < 1 || startFileIdx > len(files) {
		t.Fatalf("Invalid start file index: %d, valid range: 1-%d", startFileIdx, len(files))
	}
	if endFileIdx < startFileIdx || endFileIdx > len(files) {
		t.Fatalf("Invalid end file index: %d, valid range: %d-%d", endFileIdx, startFileIdx, len(files))
	}

	// Select files in specified range
	selectedFiles := files[startFileIdx-1 : endFileIdx]
	t.Logf("Found %d transaction files, will process files %d to %d", len(files), startFileIdx, endFileIdx)
	for i, file := range selectedFiles {
		t.Logf("Selected file %d: %s", startFileIdx+i, file)
	}

	// Process each selected file sequentially
	for i, file := range selectedFiles {
		t.Logf("Starting to process file %d/%d: %s (global index: %d)",
			i+1, len(selectedFiles), file, startFileIdx+i)

		// Get file index for loading corresponding block file
		fileIndex := compareGetFileIndex(file)

		// Load corresponding block timestamps
		err := compareLoadBlockTimestampsFromFile(dataDir, fileIndex)
		if err != nil {
			t.Logf("Failed to load block timestamps: %v", err)
			t.Logf("Will use default timestamp calculation method")
		} else {
			t.Logf("Successfully loaded block timestamps, current cached blocks: %d", len(compareBlockTimestamps))
		}

		// Open CSV file
		csvFile, err := os.Open(file)
		if err != nil {
			t.Fatalf("Cannot open CSV file %s: %v", file, err)
		}

		// Parse CSV data
		reader := csv.NewReader(csvFile)
		reader.Comma = ',' // Set delimiter to comma
		headers, err := reader.Read()
		if err != nil {
			csvFile.Close()
			t.Fatalf("Failed to read CSV header: %v", err)
		}
		t.Logf("CSV header: %v", headers)

		// Organize transactions by block
		msgsByBlock := make(map[uint64][]*core.Message)

		// Read CSV data and organize transactions
		for {
			record, err := reader.Read()
			if err != nil {
				break
			}

			// Ensure record has enough fields
			if len(record) < 10 { // At least need basic transaction fields
				t.Logf("Skipping incomplete record: %v (length: %d)", record, len(record))
				continue
			}

			// Print first few fields to confirm data format
			if record[0] == "hash" {
				// Skip header row
				continue
			}

			// Parse block number
			blockNumStr := record[3]
			blockNum, err := strconv.ParseUint(blockNumStr, 10, 64)
			if err != nil {
				t.Logf("Failed to parse block number: %v, record: %s", err, blockNumStr)
				continue
			}

			// New CSV format: hash nonce block_hash block_number transaction_index from_address to_address value gas gas_price input block_timestamp max_fee_per_gas max_priority_fee_per_gas transaction_type
			from := common.HexToAddress(record[5])
			var to *common.Address
			if record[6] != "" && record[6] != "null" {
				toAddr := common.HexToAddress(record[6])
				to = &toAddr
			}

			// Parse value
			value := new(big.Int)
			if record[7] != "" {
				value.SetString(record[7], 10)
			}

			// Parse gas
			gasLimit := uint64(21000) // Default value
			if record[8] != "" {
				gl, err := strconv.ParseUint(record[8], 10, 64)
				if err == nil && gl > 0 {
					gasLimit = gl
				}
			}

			// Parse gas price
			gasPrice := big.NewInt(1000000000) // Default value
			if record[9] != "" {
				gp := new(big.Int)
				if _, ok := gp.SetString(record[9], 10); ok && gp.Sign() > 0 {
					gasPrice = gp
				}
			}

			// Parse nonce
			nonce := uint64(0)
			if record[1] != "" {
				n, err := strconv.ParseUint(record[1], 10, 64)
				if err == nil {
					nonce = n
				}
			}

			// Parse input data
			var data []byte
			if record[10] != "" && record[10] != "null" {
				data = common.FromHex(record[10])
				// Don't know why, when creating contracts, gas consumption of code size * 200 always exceeds
				if gasLimit > 30000 && to == nil {
					gasLimit *= 10
				}
			}

			// Create message
			msg := &core.Message{
				To:               to,
				From:             from,
				Nonce:            nonce,
				Value:            value,
				GasLimit:         gasLimit,
				GasPrice:         gasPrice,
				GasFeeCap:        gasPrice, // For old transactions, use gasPrice as GasFeeCap
				GasTipCap:        gasPrice, // For old transactions, use gasPrice as GasTipCap
				Data:             data,
				SkipNonceChecks:  true,
				SkipFromEOACheck: false,
			}

			// Parse max_fee_per_gas and max_priority_fee_per_gas (if available)
			if len(record) > 12 && record[12] != "" {
				maxFeePerGas := new(big.Int)
				if _, ok := maxFeePerGas.SetString(record[12], 10); ok && maxFeePerGas.Sign() > 0 {
					msg.GasFeeCap = maxFeePerGas
				}
			}

			if len(record) > 13 && record[13] != "" {
				maxPriorityFeePerGas := new(big.Int)
				if _, ok := maxPriorityFeePerGas.SetString(record[13], 10); ok && maxPriorityFeePerGas.Sign() > 0 {
					msg.GasTipCap = maxPriorityFeePerGas
				}
			}

			msgsByBlock[blockNum] = append(msgsByBlock[blockNum], msg)
		}
		csvFile.Close() // Close CSV file

		// Calculate block range
		var minBlock, maxBlock uint64 = 1000000, 0
		for blockNum := range msgsByBlock {
			if blockNum < minBlock {
				minBlock = blockNum
			}
			if blockNum > maxBlock {
				maxBlock = blockNum
			}
		}

		// Process each block
		parent := lastProcessedBlock
		var lastCommitBlock uint64 = 0 // Record last committed block number

		ct := sdb.TrieDB().CacheTrie()
		for blockNum := minBlock; blockNum <= maxBlock; blockNum++ {
			// Counter enters new block (even for blocks without transactions)
			counter.NextBlock(blockNum)

			// Get block timestamp, use default calculation method if not available
			blockTime := uint64(blockNum * 15)
			if timestamp, ok := compareBlockTimestamps[blockNum]; ok {
				blockTime = timestamp
			}

			// Create new block
			header := &types.Header{
				ParentHash: parent.Hash(),
				Number:     new(big.Int).SetUint64(blockNum),
				GasLimit:   300000000,
				Time:       blockTime,
				Difficulty: big.NewInt(1),
				BaseFee:    big.NewInt(0),
			}

			sdb.SetBlockNum(blockNum)
			// Create statedb using previous block's state root
			statedb, err := state.New(lastStateRoot, sdb)
			if err != nil {
				t.Fatalf("Failed to create state: %v", err)
			}

			// Create statedb with counting functionality
			countingStateDB := &CompareCountingStateDB{
				StateDB: statedb,
				counter: counter,
			}

			bigBalance := new(big.Int).Mul(big.NewInt(1e15), big.NewInt(1e18))
			// Convert to uint256.Int
			balance, overflow := uint256.FromBig(bigBalance)
			if overflow {
				t.Fatalf("Balance overflow")
			}

			// Pre-allocate balance for all senders (to avoid insufficient balance since there's no incentive source)
			for _, msg := range msgsByBlock[blockNum] {
				countingStateDB.SetBalance(msg.From, balance, tracing.BalanceChangeUnspecified)
			}

			// Process all transactions in the block
			processStart := time.Now()
			gp := new(core.GasPool).AddGas(header.GasLimit)
			var usedGas uint64
			var receipts types.Receipts

			// Transaction statistics
			var successCount int
			var contractTxCount int
			var contractSuccessCount int
			var createContractCount int
			var createSuccessCount int
			var callContractCount int
			var callSuccessCount int

			// Error statistics (simplified)
			var errorCount int

			// Create EVM context
			blockContext := vm.BlockContext{
				CanTransfer: core.CanTransfer,
				Transfer:    core.Transfer,
				GetHash:     func(n uint64) common.Hash { return common.Hash{} },
				Coinbase:    common.Address{},
				BlockNumber: new(big.Int).SetUint64(blockNum),
				Time:        header.Time,
				Difficulty:  header.Difficulty,
				GasLimit:    header.GasLimit,
				BaseFee:     header.BaseFee,
			}

			// Use countingStateDB as vm.StateDB
			vmenv := vm.NewEVM(blockContext, countingStateDB, params.MainnetChainConfig, vm.Config{})

			// Process transactions if any exist
			if len(msgsByBlock[blockNum]) > 0 {
				for _, msg := range msgsByBlock[blockNum] {

					// Process transaction
					result, err := core.ApplyMessage(vmenv, msg, gp)
					var receipt *types.Receipt

					// Determine if it's a contract transaction
					isContractTx := false
					isContractCreate := false
					if msg.To == nil {
						// Contract creation
						isContractTx = true
						isContractCreate = true
						createContractCount++

						// If transaction succeeds, record created contract address
						if result != nil && result.ContractAddress != (common.Address{}) {
							counter.ContractAddresses[result.ContractAddress] = true
						}
					} else if counter.ContractAddresses[*msg.To] && len(msg.Data) > 0 {
						// Use recorded contract address to determine if it's a contract call
						isContractTx = true
						callContractCount++
					}

					if isContractTx {
						contractTxCount++
					}

					if err != nil {
						// Record error
						errorCount++

						// Create receipt
						receipt = &types.Receipt{
							Type:              types.LegacyTxType,
							Status:            types.ReceiptStatusFailed,
							CumulativeGasUsed: usedGas,
							Logs:              countingStateDB.GetLogs(common.Hash{}, blockNum, common.Hash{}),
							TxHash:            common.Hash{},
							GasUsed:           2100,
							BlockNumber:       big.NewInt(int64(blockNum)),
							BlockHash:         common.Hash{},
						}
					} else {
						// Transaction successful
						successCount++
						if isContractTx {
							contractSuccessCount++
							if isContractCreate {
								createSuccessCount++
							} else {
								callSuccessCount++
							}
						}
						usedGas += result.UsedGas

						// Create receipt
						receipt = &types.Receipt{
							Type:              types.LegacyTxType,
							Status:            types.ReceiptStatusSuccessful,
							CumulativeGasUsed: usedGas,
							Logs:              countingStateDB.GetLogs(common.Hash{}, blockNum, common.Hash{}),
							TxHash:            common.Hash{},
							GasUsed:           result.UsedGas,
							BlockNumber:       big.NewInt(int64(blockNum)),
							BlockHash:         common.Hash{},
						}
					}
					receipts = append(receipts, receipt)
				}
			}
			processDuration := time.Since(processStart)

			// Root hash generation phase
			rootGenStart := time.Now()
			var commitDuration time.Duration

			var rootGenDuration time.Duration
			root := lastStateRoot
			var cHash common.Hash
			var resultHash common.Hash
			if common.UseCacheTrie {
				cHash, resultHash, _ = countingStateDB.PreCommit(false)

				rootGenDuration = time.Since(rootGenStart)
				if cachetrie.SStart != 0 {
					// Remove waiting time (because there's no block interval, execution will be faster than global commit)
					rootGenDuration -= cachetrie.SStart
					cachetrie.SStart = 0
				}

				if resultHash != (common.Hash{}) {
					root = resultHash
					//fmt.Println(fmt.Sprintf("get  root : %v", root.String()))
				}
				codes := sdb.TrieDB().CacheTrie().PopCodes()
				if db := sdb.TrieDB().Disk(); db != nil && len(codes) > 0 {
					batch := db.NewBatch()
					for codeHash, code := range codes {
						rawdb.WriteCode(batch, codeHash, code)
					}
					if err := batch.Write(); err != nil {
						panic("write code failed")
					}
				}
				// Record deleteKVList for use in processing
				deleteKVList := countingStateDB.GetCachedDeleteKVList()
				st := func(root common.Hash, sBlockNum uint64, deleteKVList *cachetrie.DeleteKVList) {

					commitStart := time.Now()
					if deleteKVList == nil {
						return
					}
					if len(deleteKVList.Data) == 0 {
						trieDB.CacheTrie().FinishCleanup(sBlockNum, root)
						return
					}
					//t.Logf("State commit started, block number:%d, \t committed:%d, start time: %s", sBlockNum, len(deleteKVList.Data), time.Now().In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05"))

					data := deleteKVList.Data
					length := len(data)
					// Because submitting too much data at once consumes a lot of memory (verkle tree implementation issue), process in multiple batches
					chunkSize := 2000
					if length > 100000 {
						chunkSize = 10000
					}
					var newRoot = root
					for i := 0; i < length; i += chunkSize {
						end := i + chunkSize
						if end > length {
							end = length
						}
						chunk := data[i:end]
						cleanStateDB, err := state.New(newRoot, preSdb)
						// Step 2: Process all accounts
						for _, kv := range chunk {
							// Address is empty and key exists, indicating it's an account
							if (kv.Address == common.Address{}) && len(kv.Key) > 0 {
								addr := common.BytesToAddress(kv.Key)
								cleanStateDB.SetAccount(addr, kv.Value, 0)
							}
						}

						// Step 1: Process all states (storage slots)
						for _, kv := range chunk {
							// Distinguish by Address, if address is not empty, it's a storage slot
							if (kv.Address != common.Address{}) && len(kv.Key) > 0 {
								// Has address and key, indicating it's a storage slot
								addr := kv.Address
								key := common.BytesToHash(kv.Key)
								//if addr.String() == "0xcd134CE565e6b7f7CEfE2122A07A2e56D6ECbB26" && common.Bytes2Hex(kv.Key) == "0000000000000000000000000000000000000000000000000000000000000105" {
								//	addr.String()
								//}
								if common.BytesToHash(kv.Value) == (common.Hash{}) {
									cleanStateDB.SetState(addr, key, common.Hash{})
								} else {
									_, vc, _, _ := rlp.Split(kv.Value)

									value := common.BytesToHash(vc)

									// Write storage data to new stateDB
									cleanStateDB.SetState(addr, key, value)
								}
							}
						}
						// Step 3: Commit new stateDB
						newRoot, err = cleanStateDB.Commit(sBlockNum, false, false)
						if err != nil {
							t.Fatalf("Failed to commit no-cache stateDB: %v, cHash : %v", err, cHash)
						}

						// Step 4: Commit result to database
						err = preTrieDB.Commit(newRoot, false)
						if err != nil {
							t.Fatalf("Failed to commit trieDB: %v", err)
						}
					}
					commitDuration = time.Since(commitStart)

					trieDB.CacheTrie().FinishCleanup(sBlockNum, newRoot)

					if err != nil {
						t.Fatalf("Failed to commit state, block %d: %v", sBlockNum, err)
					}

					runtime.GC()
					//t.Logf("State commit completed, block number:%d, \t committed:%d, \t time:%d ", sBlockNum, len(deleteKVList.Data), commitDuration.Milliseconds())
					// Flush database to avoid excessive memory usage
					//preTrieDB.Cap(1024 * 1024 * 1024) // 1GB memory limit

				}
				if common.UserVerkle {
					// Currently, verkle tree doesn't have concurrent implementation, serial implementation will preempt resources affecting efficiency, so simulate execution here first.
					st(root, blockNum, deleteKVList)
				} else {
					go st(root, blockNum, deleteKVList)
				}
			} else {
				root, _ = countingStateDB.Commit(blockNum, false, false)

				rootGenDuration = time.Since(rootGenStart)
				// State commit to database phase - only commit when reaching configured interval
				if common.UserVerkle || blockNum-lastCommitBlock >= 10 {
					// Commit every 1000 blocks
					commitStart := time.Now()
					err = trieDB.Commit(root, false)
					commitDuration = time.Since(commitStart)
					lastCommitBlock = blockNum
				}

				if err != nil {
					t.Fatalf("Failed to commit state, block %d: %v", blockNum, err)
				}

				// Flush database to avoid excessive memory usage
				//trieDB.Cap(10 * 1024 * 1024 * 1024) // 1GB memory limit
				//}
			}

			// Update block header state root and save latest state root
			header.Root = root
			lastStateRoot = root

			// Create block
			block := types.NewBlockWithHeader(header)
			parent = block
			lastProcessedBlock = block

			// Write state root to database
			rawdb.WriteCanonicalHash(db, block.Hash(), blockNum)

			// Calculate total time and percentages
			totalTime := processDuration + rootGenDuration
			var processPercent, rootGenPercent float64

			// Recalculate time percentages, only focus on transaction processing and root hash calculation
			if totalTime > 0 {
				processPercent = float64(processDuration) / float64(totalTime) * 100
				rootGenPercent = float64(rootGenDuration) / float64(totalTime) * 100
			} else {
				// Set default values when time is 0
				processPercent = 0
				rootGenPercent = 0
			}

			// Calculate transaction success rate
			successRate := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				successRate = float64(successCount) / float64(len(msgsByBlock[blockNum]))
			}

			// Calculate contract transaction percentage
			contractTxPercent := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				contractTxPercent = float64(contractTxCount) / float64(len(msgsByBlock[blockNum]))
			}

			// Calculate contract transaction success rate
			contractSuccessRate := 0.0
			if contractTxCount > 0 {
				contractSuccessRate = float64(contractSuccessCount) / float64(contractTxCount)
			}

			// Calculate contract creation percentage and success rate
			createContractPercent := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				createContractPercent = float64(createContractCount) / float64(len(msgsByBlock[blockNum]))
			}

			createSuccessRate := 0.0
			if createContractCount > 0 {
				createSuccessRate = float64(createSuccessCount) / float64(createContractCount)
			}

			// Calculate contract call percentage and success rate
			callContractPercent := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				callContractPercent = float64(callContractCount) / float64(len(msgsByBlock[blockNum]))
			}

			callSuccessRate := 0.0
			if callContractCount > 0 {
				callSuccessRate = float64(callSuccessCount) / float64(callContractCount)
			}

			// Calculate error rate
			errorRate := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				errorRate = float64(errorCount) / float64(len(msgsByBlock[blockNum]))
			}

			// Only record statistics for blocks with transactions (maintain original behavior)
			if len(msgsByBlock[blockNum]) > 0 {
				// Create block statistics
				blockStats := CompareBlockStats{
					BlockNum:              blockNum,
					TransactionCount:      len(msgsByBlock[blockNum]),
					SuccessCount:          successCount,
					SuccessRate:           successRate,
					ContractTxCount:       contractTxCount,
					ContractTxPercent:     contractTxPercent,
					ContractSuccessCount:  contractSuccessCount,
					ContractSuccessRate:   contractSuccessRate,
					CreateContractCount:   createContractCount,
					CreateContractPercent: createContractPercent,
					CreateSuccessCount:    createSuccessCount,
					CreateSuccessRate:     createSuccessRate,
					CallContractCount:     callContractCount,
					CallContractPercent:   callContractPercent,
					CallSuccessCount:      callSuccessCount,
					CallSuccessRate:       callSuccessRate,
					ErrorCount:            errorCount,
					ErrorRate:             errorRate,
					ProcessTime:           processDuration,
					RootGenTime:           rootGenDuration,
					CommitTime:            commitDuration,
					TotalTime:             totalTime,
					ProcessTimePercent:    processPercent,
					RootGenTimePercent:    rootGenPercent,
					UniqueReads:           counter.UniqueReads,
					UniqueWrites:          counter.UniqueWrites,
				}

				// Add to statistics aggregator
				statsAgg.AddBlockStats(blockStats)

				// Record state tree statistics
				if common.UseCacheTrie && ct != nil {
					// Record CacheTrie statistics
					RecordCacheTrieStats(cacheTrieRecorder, blockNum, counter.UniqueWrites, counter.UniqueReads,
						len(msgsByBlock[blockNum]), processDuration, rootGenDuration, ct)

				} else if trieDB.IsVerkle() {
					// Record VerkleTrie statistics
					RecordVerkleTrieStats(verkleTrieRecorder, blockNum, counter.UniqueWrites, counter.UniqueReads,
						len(msgsByBlock[blockNum]), processDuration, rootGenDuration)
				} else {
					// Record StandardTrie statistics
					RecordTrieStats(standardTrieRecorder, blockNum, counter.UniqueWrites, counter.UniqueReads,
						len(msgsByBlock[blockNum]), processDuration, rootGenDuration)
				}
			}
		}
		t.Logf("Completed processing file: %s", file)
	}

	// Save final state root to database for next checkpoint continuation
	t.Logf("Saving final state root to database: %s", lastStateRoot.String())
	rawdb.WriteLastRunStateRoot(db, lastStateRoot)

	// Output final statistics after processing completion
	statsAgg.PrintStats()

	// Output final state tree statistics before function ends
	t.Logf("File range %d to %d processing completed, final state root: %s", startFileIdx, endFileIdx, lastStateRoot.String())
}
