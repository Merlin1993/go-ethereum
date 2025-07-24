package tree

import (
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

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

// TestCompareProcessTransactions 测试处理CSV中的交易
func TestCompareProcessTransactions(t *testing.T) {
	// 定义数据库路径
	dbDir := "F:\\ethdata\\geth_compare_db_swmt"
	statsDir := "F:\\ethdata\\compare_stats_2"
	dataDir := "E:\\ethdata"

	// 指定文件范围，硬编码方式指定起始和结束文件索引
	startFileIdx := 1 // 起始文件索引（从1开始）
	endFileIdx := 1   // 结束文件索引
	//46147
	var startNum uint64 = 46147

	// 创建统计聚合器
	statsAgg := NewCompareStatsAggregator(statsDir, 100000) // 使用直接数值替代常量

	// 添加: 创建状态树统计记录器
	trieStatsDir := filepath.Join(statsDir, "trie_stats")
	standardTrieRecorder := CreateTrieStatsRecorder(trieStatsDir, dbDir, StandardTrie)
	cacheTrieRecorder := CreateTrieStatsRecorder(trieStatsDir, dbDir, CacheTrie)
	verkleTrieRecorder := CreateTrieStatsRecorder(trieStatsDir, dbDir, VerkleTrie)

	// 创建或打开持久化数据库
	ldb, err := leveldb.New(dbDir, 1024, 1024, "eth-compare-process-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
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

	// 使用正确的state包API
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

	// 创建genesis区块和区块链
	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{},
	}
	genesis := gspec.MustCommit(db, trieDB)

	// 保存最后处理的区块和状态根
	lastProcessedBlock := genesis
	var lastStateRoot common.Hash

	// 从数据库中尝试读取上次运行的状态根，支持断点续跑
	lastRunRoot := rawdb.ReadLastRunStateRoot(db)
	if lastRunRoot != (common.Hash{}) {
		// 找到了上次运行的状态根，使用它作为起点
		t.Logf("发现上次运行的状态根: %s，将从此状态继续处理", lastRunRoot.String())
		lastStateRoot = lastRunRoot
	} else {
		// 使用创世区块的状态根
		if genesis != nil {
			lastStateRoot = genesis.Root()
		}
		t.Logf("未发现上次运行状态根，使用创世区块状态根: %s", lastStateRoot.String())
	}

	// 创建状态访问计数器
	counter := NewCompareStateAccessCounter()

	// 查找所有交易文件
	files, err := compareFindTransactionFiles(dataDir)
	if err != nil {
		t.Fatalf("查找CSV文件失败: %v", err)
	}

	if len(files) == 0 {
		t.Fatalf("未找到任何交易文件")
	}

	// 校验文件范围
	if startFileIdx < 1 || startFileIdx > len(files) {
		t.Fatalf("起始文件索引无效: %d, 有效范围: 1-%d", startFileIdx, len(files))
	}
	if endFileIdx < startFileIdx || endFileIdx > len(files) {
		t.Fatalf("结束文件索引无效: %d, 有效范围: %d-%d", endFileIdx, startFileIdx, len(files))
	}

	// 选择指定范围的文件
	selectedFiles := files[startFileIdx-1 : endFileIdx]
	t.Logf("找到 %d 个交易文件，将处理 %d 到 %d 号文件", len(files), startFileIdx, endFileIdx)
	for i, file := range selectedFiles {
		t.Logf("选中文件 %d: %s", startFileIdx+i, file)
	}

	// 依次处理每个选中的文件
	for i, file := range selectedFiles {
		t.Logf("开始处理第 %d/%d 个文件: %s (全局索引: %d)",
			i+1, len(selectedFiles), file, startFileIdx+i)

		// 获取文件索引，用于加载对应的区块文件
		fileIndex := compareGetFileIndex(file)

		// 加载对应的区块时间戳
		err := compareLoadBlockTimestampsFromFile(dataDir, fileIndex)
		if err != nil {
			t.Logf("加载区块时间戳失败: %v", err)
			t.Logf("将使用默认时间戳计算方式")
		} else {
			t.Logf("成功加载区块时间戳，当前缓存区块数: %d", len(compareBlockTimestamps))
		}

		// 打开CSV文件
		csvFile, err := os.Open(file)
		if err != nil {
			t.Fatalf("无法打开CSV文件 %s: %v", file, err)
		}

		// 解析CSV数据
		reader := csv.NewReader(csvFile)
		reader.Comma = ',' // 设置分隔符为逗号
		headers, err := reader.Read()
		if err != nil {
			csvFile.Close()
			t.Fatalf("读取CSV头失败: %v", err)
		}
		t.Logf("CSV头: %v", headers)

		// 按区块组织交易
		msgsByBlock := make(map[uint64][]*core.Message)

		// 读取CSV数据并组织交易
		for {
			record, err := reader.Read()
			if err != nil {
				break
			}

			// 确保记录有足够的字段
			if len(record) < 10 { // 至少需要基本交易字段
				t.Logf("跳过不完整的记录: %v (长度: %d)", record, len(record))
				continue
			}

			// 打印前几个字段，确认数据格式
			if record[0] == "hash" {
				// 跳过标题行
				continue
			}

			// 解析区块号
			blockNumStr := record[3]
			blockNum, err := strconv.ParseUint(blockNumStr, 10, 64)
			if err != nil {
				t.Logf("解析区块号失败: %v, 记录: %s", err, blockNumStr)
				continue
			}

			// 新CSV格式: hash nonce block_hash block_number transaction_index from_address to_address value gas gas_price input block_timestamp max_fee_per_gas max_priority_fee_per_gas transaction_type
			from := common.HexToAddress(record[5])
			var to *common.Address
			if record[6] != "" && record[6] != "null" {
				toAddr := common.HexToAddress(record[6])
				to = &toAddr
			}

			// 解析value
			value := new(big.Int)
			if record[7] != "" {
				value.SetString(record[7], 10)
			}

			// 解析gas
			gasLimit := uint64(21000) // 默认值
			if record[8] != "" {
				gl, err := strconv.ParseUint(record[8], 10, 64)
				if err == nil && gl > 0 {
					gasLimit = gl
				}
			}

			// 解析gas price
			gasPrice := big.NewInt(1000000000) // 默认值
			if record[9] != "" {
				gp := new(big.Int)
				if _, ok := gp.SetString(record[9], 10); ok && gp.Sign() > 0 {
					gasPrice = gp
				}
			}

			// 解析nonce
			nonce := uint64(0)
			if record[1] != "" {
				n, err := strconv.ParseUint(record[1], 10, 64)
				if err == nil {
					nonce = n
				}
			}

			// 解析input数据
			var data []byte
			if record[10] != "" && record[10] != "null" {
				data = common.FromHex(record[10])
				// 不知道为啥，合约创建的时候，code的大小乘以200的gas消耗老是超
				if gasLimit > 30000 && to == nil {
					gasLimit *= 10
				}
			}

			// 创建消息
			msg := &core.Message{
				To:               to,
				From:             from,
				Nonce:            nonce,
				Value:            value,
				GasLimit:         gasLimit,
				GasPrice:         gasPrice,
				GasFeeCap:        gasPrice, // 对于旧交易，使用gasPrice作为GasFeeCap
				GasTipCap:        gasPrice, // 对于旧交易，使用gasPrice作为GasTipCap
				Data:             data,
				SkipNonceChecks:  true,
				SkipFromEOACheck: false,
			}

			// 解析max_fee_per_gas和max_priority_fee_per_gas（如果有）
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
		csvFile.Close() // 关闭CSV文件

		// 计算区块范围
		var minBlock, maxBlock uint64 = 1000000, 0
		for blockNum := range msgsByBlock {
			if blockNum < minBlock {
				minBlock = blockNum
			}
			if blockNum > maxBlock {
				maxBlock = blockNum
			}
		}

		// 处理每个区块
		parent := lastProcessedBlock
		var lastCommitBlock uint64 = 0 // 记录上次提交的区块号

		ct := sdb.TrieDB().CacheTrie()
		for blockNum := minBlock; blockNum <= maxBlock; blockNum++ {
			if len(msgsByBlock[blockNum]) == 0 {
				continue
			}

			// 计数器进入新区块
			counter.NextBlock(blockNum)

			// 获取区块时间戳，如果没有则使用默认计算方式
			blockTime := uint64(blockNum * 15)
			if timestamp, ok := compareBlockTimestamps[blockNum]; ok {
				blockTime = timestamp
			}

			// 创建新的区块
			header := &types.Header{
				ParentHash: parent.Hash(),
				Number:     new(big.Int).SetUint64(blockNum),
				GasLimit:   300000000,
				Time:       blockTime,
				Difficulty: big.NewInt(1),
				BaseFee:    big.NewInt(0),
			}

			sdb.SetBlockNum(blockNum)
			// 创建statedb，使用上一个区块的状态根
			statedb, err := state.New(lastStateRoot, sdb)
			if err != nil {
				t.Fatalf("创建状态失败: %v", err)
			}

			// 创建带计数功能的statedb
			countingStateDB := &CompareCountingStateDB{
				StateDB: statedb,
				counter: counter,
			}

			bigBalance := new(big.Int).Mul(big.NewInt(1e15), big.NewInt(1e18))
			// 转换为uint256.Int
			balance, overflow := uint256.FromBig(bigBalance)
			if overflow {
				t.Fatalf("余额溢出")
			}

			// 为所有发送方预分配余额(因为没有激励来源，避免触发余额不足)
			for _, msg := range msgsByBlock[blockNum] {
				countingStateDB.SetBalance(msg.From, balance, tracing.BalanceChangeUnspecified)
			}

			// 处理区块中的所有交易
			processStart := time.Now()
			gp := new(core.GasPool).AddGas(header.GasLimit)
			var usedGas uint64
			var receipts types.Receipts

			// 交易统计
			var successCount int
			var contractTxCount int
			var contractSuccessCount int
			var createContractCount int
			var createSuccessCount int
			var callContractCount int
			var callSuccessCount int

			// 错误统计（简化）
			var errorCount int

			// 创建EVM上下文
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

			// 使用countingStateDB作为vm.StateDB
			vmenv := vm.NewEVM(blockContext, countingStateDB, params.MainnetChainConfig, vm.Config{})

			for _, msg := range msgsByBlock[blockNum] {

				// 处理交易
				result, err := core.ApplyMessage(vmenv, msg, gp)
				var receipt *types.Receipt

				// 判断是否为合约交易
				isContractTx := false
				isContractCreate := false
				if msg.To == nil {
					// 合约创建
					isContractTx = true
					isContractCreate = true
					createContractCount++

					// 如果交易成功，记录创建的合约地址
					if result != nil && result.ContractAddress != (common.Address{}) {
						counter.ContractAddresses[result.ContractAddress] = true
					}
				} else if counter.ContractAddresses[*msg.To] && len(msg.Data) > 0 {
					// 使用记录的合约地址判断是否为合约调用
					isContractTx = true
					callContractCount++
				}

				if isContractTx {
					contractTxCount++
				}

				if err != nil {
					// 记录错误
					errorCount++

					// 创建收据
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
					// 交易成功
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

					// 创建收据
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
			processDuration := time.Since(processStart)

			// 生成根哈希阶段
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
					//去除等待的时间（因为没有区块间隔，执行比全局承诺会偏快）
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
				// 记录deleteKVList以便在处理中使用
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
					t.Logf("提交状态开始，区块号:%d, \t 提交了:%d, 起始时间：%s", sBlockNum, len(deleteKVList.Data), time.Now().In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05"))

					data := deleteKVList.Data
					length := len(data)
					// 因为一次提交数据太大会比较吃内存（verkle树实现问题），这里分多次处理
					chunkSize := 5000
					if length > 500000 {
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
						// 第二步：处理所有账户
						for _, kv := range chunk {
							// 地址为空且键存在，说明是账户
							if (kv.Address == common.Address{}) && len(kv.Key) > 0 {
								addr := common.BytesToAddress(kv.Key)
								cleanStateDB.SetAccount(addr, kv.Value, 0)
							}
						}

						// 第一步：处理所有状态（存储槽）
						for _, kv := range chunk {
							// 通过Address区分是否有地址，如果地址非空，则是存储槽
							if (kv.Address != common.Address{}) && len(kv.Key) > 0 {
								// 有地址且有键，说明是存储槽
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

									// 将存储数据写入新stateDB
									cleanStateDB.SetState(addr, key, value)
								}
							}
						}
						// 第三步：对新stateDB进行commit
						newRoot, err = cleanStateDB.Commit(sBlockNum, false, false)
						if err != nil {
							t.Fatalf("提交无cache stateDB失败: %v, cHash : %v", err, cHash)
						}

						// 第四步：将结果提交到数据库
						err = preTrieDB.Commit(newRoot, false)
						if err != nil {
							t.Fatalf("提交trieDB失败: %v", err)
						}
					}
					commitDuration = time.Since(commitStart)

					trieDB.CacheTrie().FinishCleanup(sBlockNum, newRoot)

					if err != nil {
						t.Fatalf("提交状态失败，区块 %d: %v", sBlockNum, err)
					}

					runtime.GC()
					t.Logf("提交状态完成，区块号:%d, \t 提交了:%d, \t 时间:%d ", sBlockNum, len(deleteKVList.Data), commitDuration.Milliseconds())
					// 刷新数据库，避免内存占用过大
					//preTrieDB.Cap(1024 * 1024 * 1024) // 1GB内存限制

				}
				if common.UserVerkle {
					//目前而言，verkle树未完成并发实现，串行实现会抢占资源导致影响效率，所以这里先模拟执行。
					st(root, blockNum, deleteKVList)
				} else {
					go st(root, blockNum, deleteKVList)
				}
			} else {
				root, _ = countingStateDB.Commit(blockNum, false, false)

				rootGenDuration = time.Since(rootGenStart)
				// 提交状态到数据库阶段 - 只在达到配置的间隔时才提交
				if blockNum-lastCommitBlock >= 5000 { // 每1000个区块提交一次
				}
				commitStart := time.Now()
				err = trieDB.Commit(root, false)
				commitDuration = time.Since(commitStart)
				lastCommitBlock = blockNum

				if err != nil {
					t.Fatalf("提交状态失败，区块 %d: %v", blockNum, err)
				}

				// 刷新数据库，避免内存占用过大
				trieDB.Cap(10 * 1024 * 1024 * 1024) // 1GB内存限制
				//}
			}

			// 更新区块头的状态根和保存最新状态根
			header.Root = root
			lastStateRoot = root

			// 创建区块
			block := types.NewBlockWithHeader(header)
			parent = block
			lastProcessedBlock = block

			// 将状态根写入数据库
			rawdb.WriteCanonicalHash(db, block.Hash(), blockNum)

			// 计算总时间和百分比
			totalTime := processDuration + rootGenDuration
			var processPercent, rootGenPercent float64

			// 重新计算时间百分比，只关注交易处理和根哈希计算
			if totalTime > 0 {
				processPercent = float64(processDuration) / float64(totalTime) * 100
				rootGenPercent = float64(rootGenDuration) / float64(totalTime) * 100
			} else {
				// 时间为0时设置默认值
				processPercent = 0
				rootGenPercent = 0
			}

			// 计算交易成功率
			successRate := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				successRate = float64(successCount) / float64(len(msgsByBlock[blockNum]))
			}

			// 计算合约交易占比
			contractTxPercent := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				contractTxPercent = float64(contractTxCount) / float64(len(msgsByBlock[blockNum]))
			}

			// 计算合约交易成功率
			contractSuccessRate := 0.0
			if contractTxCount > 0 {
				contractSuccessRate = float64(contractSuccessCount) / float64(contractTxCount)
			}

			// 计算合约创建占比和成功率
			createContractPercent := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				createContractPercent = float64(createContractCount) / float64(len(msgsByBlock[blockNum]))
			}

			createSuccessRate := 0.0
			if createContractCount > 0 {
				createSuccessRate = float64(createSuccessCount) / float64(createContractCount)
			}

			// 计算合约调用占比和成功率
			callContractPercent := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				callContractPercent = float64(callContractCount) / float64(len(msgsByBlock[blockNum]))
			}

			callSuccessRate := 0.0
			if callContractCount > 0 {
				callSuccessRate = float64(callSuccessCount) / float64(callContractCount)
			}

			// 计算错误率
			errorRate := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				errorRate = float64(errorCount) / float64(len(msgsByBlock[blockNum]))
			}

			// 创建区块统计数据
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

			// 添加到统计聚合器
			statsAgg.AddBlockStats(blockStats)

			// 记录状态树统计
			if common.UseCacheTrie && ct != nil {
				// 记录CacheTrie统计
				RecordCacheTrieStats(cacheTrieRecorder, blockNum, counter.UniqueWrites, counter.UniqueReads,
					len(msgsByBlock[blockNum]), processDuration, rootGenDuration, ct)

			} else if trieDB.IsVerkle() {
				// 记录VerkleTrie统计
				RecordVerkleTrieStats(verkleTrieRecorder, blockNum, counter.UniqueWrites, counter.UniqueReads,
					len(msgsByBlock[blockNum]), processDuration, rootGenDuration)
			} else {
				// 记录StandardTrie统计
				RecordTrieStats(standardTrieRecorder, blockNum, counter.UniqueWrites, counter.UniqueReads,
					len(msgsByBlock[blockNum]), processDuration, rootGenDuration)
			}
		}
		t.Logf("完成处理文件: %s", file)
	}

	// 将最后的状态根保存到数据库，用于下次断点续跑
	t.Logf("保存最终状态根到数据库: %s", lastStateRoot.String())
	rawdb.WriteLastRunStateRoot(db, lastStateRoot)

	// 处理完成后输出最终统计信息
	statsAgg.PrintStats()

	// 在函数结束前输出最终状态树统计
	t.Logf("文件范围 %d 到 %d 处理完成，最终状态根: %s", startFileIdx, endFileIdx, lastStateRoot.String())
}
