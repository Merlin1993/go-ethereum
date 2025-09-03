package tree_test

import (
	"encoding/csv"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-verkle"
	"github.com/holiman/uint256"
)

// 测试配置
const (
	// 证明测试配置
	proofTestDataSize = 100000 // 测试数据量
	proofBatchSize    = 10000  // 批量处理大小
	proofTestRounds   = 5      // 测试轮数

	// 目录配置
	mptProofDir    = "F:\\ethdata\\stree2\\mpt_proof"
	verkleProofDir = "F:\\ethdata\\stree2\\verkle_proof"

	// 主网数据目录
	mainnetDataDir = "E:\\ethdata" // 主网交易数据目录
)

// ProofPerformanceStats 证明性能统计
type ProofPerformanceStats struct {
	TreeType      string        // 树类型 (MPT/Verkle)
	DataSize      int           // 数据量
	ProofCount    int           // 证明数量
	GenTime       time.Duration // 生成证明总时间
	VerifyTime    time.Duration // 验证证明总时间
	AvgGenTime    time.Duration // 平均生成时间
	AvgVerifyTime time.Duration // 平均验证时间
	ProofSize     int64         // 证明总大小
	AvgProofSize  float64       // 平均证明大小
	NodeCount     uint64        // 节点数量
	BlockNumber   uint64        // 区块号
}

// TestMPTProofWithMainnetData 使用主网真实交易数据测试MPT树证明性能
func TestMPTProofWithMainnetData(t *testing.T) {
	// 创建临时目录
	os.MkdirAll(mptProofDir, os.ModePerm)

	// 创建数据库
	ldb, err := leveldb.New(mptProofDir, 128, 128, "mpt-mainnet-proof-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	// 创建Trie数据库
	cacheConfig := core.DefaultCacheConfigWithScheme(rawdb.HashScheme)
	cacheConfig.SnapshotLimit = 0
	diskDB := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(diskDB, cacheConfig.TriedbConfig(true))

	// 创建创世块和区块链
	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{},
	}
	genesis := gspec.MustCommit(diskDB, trieDB)

	// 查找主网交易数据文件
	files, err := findMainnetTransactionFiles(mainnetDataDir)
	if err != nil {
		t.Fatalf("查找主网交易文件失败: %v", err)
	}

	if len(files) == 0 {
		t.Fatalf("未找到主网交易文件")
	}

	// 选择前几个文件进行测试
	testFiles := files[:min(3, len(files))]
	t.Logf("选择 %d 个文件进行测试", len(testFiles))

	var allStats []ProofPerformanceStats
	parent := genesis
	var lastStateRoot common.Hash = genesis.Root

	// 处理每个文件
	for fileIdx, file := range testFiles {
		t.Logf("处理文件 %d/%d: %s", fileIdx+1, len(testFiles), filepath.Base(file))

		// 加载交易数据
		transactions, err := loadMainnetTransactions(file)
		if err != nil {
			t.Logf("加载交易文件失败: %v, 跳过", err)
			continue
		}

		// 按区块组织交易
		transactionsByBlock := organizeTransactionsByBlock(transactions)

		// 处理每个区块
		for blockNum, txs := range transactionsByBlock {
			if len(txs) == 0 {
				continue
			}

			t.Logf("处理区块 %d, 包含 %d 笔交易", blockNum, len(txs))

			// 创建新区块
			header := &types.Header{
				ParentHash: parent.Hash(),
				Number:     new(big.Int).SetUint64(blockNum),
				GasLimit:   300000000,
				Time:       uint64(blockNum * 15),
				Difficulty: big.NewInt(1),
				BaseFee:    big.NewInt(0),
			}

			// 创建状态数据库
			statedb, err := state.New(lastStateRoot, state.NewDatabase(trieDB))
			if err != nil {
				t.Fatalf("创建状态数据库失败: %v", err)
			}

			// 为所有发送者预分配余额
			bigBalance := new(big.Int).Mul(big.NewInt(1e15), big.NewInt(1e18))
			balance, overflow := uint256.FromBig(bigBalance)
			if overflow {
				t.Fatalf("余额溢出")
			}

			for _, tx := range txs {
				statedb.SetBalance(tx.From, balance, 0)
			}

			// 处理区块中的交易
			processStart := time.Now()
			gp := new(core.GasPool).AddGas(header.GasLimit)

			// 创建EVM上下文
			blockContext := core.NewEVMBlockContext(header, nil, &common.Address{})
			evm := vm.NewEVM(blockContext, statedb, params.TestChainConfig, vm.Config{})

			// 执行交易
			for txIdx, tx := range txs {
				msg := &core.Message{
					To:               tx.To,
					From:             tx.From,
					Nonce:            tx.Nonce,
					Value:            tx.Value,
					GasLimit:         tx.GasLimit,
					GasPrice:         tx.GasPrice,
					GasFeeCap:        tx.GasPrice,
					GasTipCap:        tx.GasPrice,
					Data:             tx.Data,
					SkipNonceChecks:  true,
					SkipFromEOACheck: false,
				}

				statedb.Prepare(common.Hash{}, blockNum, txIdx)
				statedb.SetNonce(tx.From, tx.Nonce+1)

				// 执行交易
				_, err := core.ApplyMessage(evm, msg, gp)
				if err != nil {
					t.Logf("交易执行失败: %v", err)
					continue
				}
			}

			// 提交状态
			root, err := statedb.Commit(blockNum, false, false)
			if err != nil {
				t.Fatalf("提交状态失败: %v", err)
			}

			// 提交到Trie数据库
			if err := trieDB.Commit(root, false); err != nil {
				t.Fatalf("提交Trie数据库失败: %v", err)
			}

			processTime := time.Since(processStart)
			t.Logf("区块 %d 处理完成，耗时: %v", blockNum, processTime)

			// 更新状态
			parent = &types.Block{
				Header: header,
				Body:   &types.Body{Transactions: txs},
			}
			lastStateRoot = root

			// 测试证明生成
			if len(txs) > 0 {
				stats := testMPTProofGeneration(t, trieDB, root, txs, blockNum)
				allStats = append(allStats, stats)
			}
		}
	}

	// 输出性能对比
	printProofPerformanceComparison(t, allStats)

	// 保存结果到CSV
	saveProofResultsToCSV(t, "mpt_mainnet_proof_performance.csv", allStats)
}

// TestVerkleProofWithMainnetData 使用主网真实交易数据测试Verkle树证明性能
func TestVerkleProofWithMainnetData(t *testing.T) {
	// 创建临时目录
	os.MkdirAll(verkleProofDir, os.ModePerm)

	// 创建数据库
	ldb, err := leveldb.New(verkleProofDir, 128, 128, "verkle-mainnet-proof-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	// 创建Trie数据库
	cacheConfig := core.DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	diskDB := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(diskDB, cacheConfig.TriedbConfig(true))

	// 创建创世块和区块链
	gspec := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc:  core.GenesisAlloc{},
	}
	genesis := gspec.MustCommit(diskDB, trieDB)

	// 查找主网交易数据文件
	files, err := findMainnetTransactionFiles(mainnetDataDir)
	if err != nil {
		t.Fatalf("查找主网交易文件失败: %v", err)
	}

	if len(files) == 0 {
		t.Fatalf("未找到主网交易文件")
	}

	// 选择前几个文件进行测试
	testFiles := files[:min(3, len(files))]
	t.Logf("选择 %d 个文件进行测试", len(testFiles))

	var allStats []ProofPerformanceStats
	parent := genesis
	var lastStateRoot common.Hash = genesis.Root()

	// 创建point cache
	pointCache := utils.NewPointCache(1024)

	// 处理每个文件
	for fileIdx, file := range testFiles {
		t.Logf("处理文件 %d/%d: %s", fileIdx+1, len(testFiles), filepath.Base(file))

		// 加载交易数据
		transactions, err := loadMainnetTransactions(file)
		if err != nil {
			t.Logf("加载交易文件失败: %v, 跳过", err)
			continue
		}

		// 按区块组织交易
		transactionsByBlock := organizeTransactionsByBlock(transactions)

		// 处理每个区块
		for blockNum, txs := range transactionsByBlock {
			if len(txs) == 0 {
				continue
			}

			t.Logf("处理区块 %d, 包含 %d 笔交易", blockNum, len(txs))

			// 创建新区块
			header := &types.Header{
				ParentHash: parent.Hash(),
				Number:     new(big.Int).SetUint64(blockNum),
				GasLimit:   300000000,
				Time:       uint64(blockNum * 15),
				Difficulty: big.NewInt(1),
				BaseFee:    big.NewInt(0),
			}

			// 创建状态数据库
			statedb, err := state.New(lastStateRoot, state.NewDatabase(trieDB, nil))
			if err != nil {
				t.Fatalf("创建状态数据库失败: %v", err)
			}

			// 为所有发送者预分配余额
			bigBalance := new(big.Int).Mul(big.NewInt(1e15), big.NewInt(1e18))
			balance, overflow := uint256.FromBig(bigBalance)
			if overflow {
				t.Fatalf("余额溢出")
			}

			for _, tx := range txs {
				statedb.SetBalance(tx.From, balance, 0)
			}

			// 处理区块中的交易
			processStart := time.Now()
			gp := new(core.GasPool).AddGas(header.GasLimit)

			// 创建EVM上下文
			blockContext := core.NewEVMBlockContext(header, nil, &common.Address{})
			evm := vm.NewEVM(blockContext, vm.TxContext{}, statedb, params.TestChainConfig, vm.Config{})

			// 执行交易
			for txIdx, tx := range txs {
				msg := &core.Message{
					To:               tx.To,
					From:             tx.From,
					Nonce:            tx.Nonce,
					Value:            tx.Value,
					GasLimit:         tx.GasLimit,
					GasPrice:         tx.GasPrice,
					GasFeeCap:        tx.GasPrice,
					GasTipCap:        tx.GasPrice,
					Data:             tx.Data,
					SkipNonceChecks:  true,
					SkipFromEOACheck: false,
				}

				statedb.Prepare(tx.Hash(), blockNum, txIdx)
				statedb.SetNonce(tx.From, tx.Nonce+1)

				// 执行交易
				_, err := core.ApplyMessage(evm, msg, gp)
				if err != nil {
					t.Logf("交易执行失败: %v", err)
					continue
				}
			}

			// 提交状态
			root, err := statedb.Commit(blockNum, false, false)
			if err != nil {
				t.Fatalf("提交状态失败: %v", err)
			}

			// 提交到Trie数据库
			if err := trieDB.Commit(root, false); err != nil {
				t.Fatalf("提交Trie数据库失败: %v", err)
			}

			processTime := time.Since(processStart)
			t.Logf("区块 %d 处理完成，耗时: %v", blockNum, processTime)

			// 更新状态
			parent = &types.Block{
				Header: header,
				Body:   &types.Body{Transactions: txs},
			}
			lastStateRoot = root

			// 测试证明生成
			if len(txs) > 0 {
				stats := testVerkleProofGeneration(t, trieDB, root, txs, blockNum, pointCache)
				allStats = append(allStats, stats)
			}
		}
	}

	// 输出性能对比
	printProofPerformanceComparison(t, allStats)

	// 保存结果到CSV
	saveProofResultsToCSV(t, "verkle_mainnet_proof_performance.csv", allStats)
}

// 辅助函数和数据结构

// MainnetTransaction 主网交易数据结构
type MainnetTransaction struct {
	Hash     common.Hash
	Nonce    uint64
	BlockNum uint64
	From     common.Address
	To       *common.Address
	Value    *big.Int
	GasLimit uint64
	GasPrice *big.Int
	Data     []byte
}

// findMainnetTransactionFiles 查找主网交易数据文件
func findMainnetTransactionFiles(dataDir string) ([]string, error) {
	var files []string

	// 查找CSV文件
	err := filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".csv" {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}

// loadMainnetTransactions 从CSV文件加载主网交易数据
func loadMainnetTransactions(filename string) ([]MainnetTransaction, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ','

	// 读取表头
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	var transactions []MainnetTransaction

	// 读取数据行
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		if len(record) < 10 {
			continue
		}

		// 跳过表头行
		if record[0] == "hash" {
			continue
		}

		// 解析交易数据
		tx := MainnetTransaction{}

		// 解析哈希
		if record[0] != "" {
			tx.Hash = common.HexToHash(record[0])
		}

		// 解析nonce
		if record[1] != "" {
			if nonce, err := strconv.ParseUint(record[1], 10, 64); err == nil {
				tx.Nonce = nonce
			}
		}

		// 解析区块号
		if record[3] != "" {
			if blockNum, err := strconv.ParseUint(record[3], 10, 64); err == nil {
				tx.BlockNum = blockNum
			}
		}

		// 解析发送地址
		if record[5] != "" {
			tx.From = common.HexToAddress(record[5])
		}

		// 解析接收地址
		if record[6] != "" && record[6] != "null" {
			toAddr := common.HexToAddress(record[6])
			tx.To = &toAddr
		}

		// 解析价值
		if record[7] != "" {
			tx.Value = new(big.Int)
			tx.Value.SetString(record[7], 10)
		} else {
			tx.Value = big.NewInt(0)
		}

		// 解析gas限制
		if record[8] != "" {
			if gasLimit, err := strconv.ParseUint(record[8], 10, 64); err == nil && gasLimit > 0 {
				tx.GasLimit = gasLimit
			} else {
				tx.GasLimit = 21000
			}
		} else {
			tx.GasLimit = 21000
		}

		// 解析gas价格
		if record[9] != "" {
			tx.GasPrice = new(big.Int)
			if _, ok := tx.GasPrice.SetString(record[9], 10); !ok || tx.GasPrice.Sign() <= 0 {
				tx.GasPrice = big.NewInt(1000000000)
			}
		} else {
			tx.GasPrice = big.NewInt(1000000000)
		}

		// 解析输入数据
		if record[10] != "" && record[10] != "null" {
			tx.Data = common.FromHex(record[10])
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// organizeTransactionsByBlock 按区块组织交易
func organizeTransactionsByBlock(transactions []MainnetTransaction) map[uint64][]MainnetTransaction {
	result := make(map[uint64][]MainnetTransaction)

	for _, tx := range transactions {
		result[tx.BlockNum] = append(result[tx.BlockNum], tx)
	}

	return result
}

// testMPTProofGeneration 测试MPT证明生成
func testMPTProofGeneration(t *testing.T, trieDB *triedb.Database, root common.Hash, txs []MainnetTransaction, blockNum uint64) ProofPerformanceStats {
	// 重新打开Trie以进行证明生成
	tr, err := trie.New(trie.TrieID(root), trieDB)
	if err != nil {
		t.Fatalf("重新打开Trie失败: %v", err)
	}

	// 选择一些交易进行证明测试
	proofCount := min(100, len(txs))
	selectedTxs := txs[:proofCount]

	// 生成证明
	proofStart := time.Now()
	proofDB := trienode.NewProofSet()

	// 为每个交易的发送地址生成证明
	for _, tx := range selectedTxs {
		// 使用地址作为键来生成证明
		key := tx.From.Bytes()
		if err := tr.Prove(key, proofDB); err != nil {
			t.Logf("生成证明失败: %v", err)
			continue
		}
	}

	proofGenTime := time.Since(proofStart)

	// 验证证明
	verifyStart := time.Now()
	verifyCount := 0
	for _, tx := range selectedTxs[:min(10, len(selectedTxs))] {
		key := tx.From.Bytes()
		value, err := trie.VerifyProof(root, key, proofDB)
		if err != nil {
			t.Logf("验证证明失败: %v", err)
			continue
		}
		if value != nil {
			verifyCount++
		}
	}

	verifyTime := time.Since(verifyStart)

	// 计算统计信息
	stats := ProofPerformanceStats{
		TreeType:      "MPT",
		DataSize:      len(txs),
		ProofCount:    proofCount,
		GenTime:       proofGenTime,
		VerifyTime:    verifyTime,
		AvgGenTime:    proofGenTime / time.Duration(proofCount),
		AvgVerifyTime: verifyTime / time.Duration(verifyCount),
		ProofSize:     int64(proofDB.DataSize()),
		AvgProofSize:  float64(proofDB.DataSize()) / float64(proofCount),
		NodeCount:     uint64(proofDB.KeyCount()),
		BlockNumber:   blockNum,
	}

	t.Logf("区块 %d: 生成 %d 个证明，耗时: %v (平均: %v)",
		blockNum, proofCount, proofGenTime, stats.AvgGenTime)
	t.Logf("验证 %d 个证明，耗时: %v (平均: %v)",
		verifyCount, verifyTime, stats.AvgVerifyTime)
	t.Logf("证明大小: %s (平均: %.2f bytes/proof)",
		bytesToReadable(uint64(proofDB.DataSize())), stats.AvgProofSize)

	return stats
}

// testVerkleProofGeneration 测试Verkle证明生成
func testVerkleProofGeneration(t *testing.T, trieDB *triedb.Database, root common.Hash, txs []MainnetTransaction, blockNum uint64, pointCache *utils.PointCache) ProofPerformanceStats {
	// 重新打开Verkle Trie以进行证明生成
	vt, err := trie.NewVerkleTrie(root, trieDB, pointCache)
	if err != nil {
		t.Fatalf("重新打开Verkle Trie失败: %v", err)
	}

	// 选择一些交易进行证明测试
	proofCount := min(100, len(txs))
	selectedTxs := txs[:proofCount]

	// 生成证明
	proofStart := time.Now()

	// 创建空的post trie用于证明生成
	emptyVerkleTrie, err := trie.NewVerkleTrie(common.Hash{}, trieDB, pointCache)
	if err != nil {
		t.Fatalf("创建空Verkle Trie失败: %v", err)
	}

	// 收集需要证明的键
	var keys [][]byte
	for _, tx := range selectedTxs {
		// 使用地址作为键
		keys = append(keys, tx.From.Bytes())
	}

	proof, stateDiff, err := vt.Proof(emptyVerkleTrie, keys)
	if err != nil {
		t.Fatalf("生成Verkle证明失败: %v", err)
	}

	proofGenTime := time.Since(proofStart)

	// 验证证明
	verifyStart := time.Now()

	// 序列化证明以获取大小
	proofBytes, err := proof.MarshalJSON()
	if err != nil {
		t.Fatalf("序列化证明失败: %v", err)
	}

	// 验证证明
	err = verkle.Verify(proof, root.Bytes(), root.Bytes(), stateDiff)
	if err != nil {
		t.Fatalf("验证Verkle证明失败: %v", err)
	}

	verifyTime := time.Since(verifyStart)

	// 计算统计信息
	stats := ProofPerformanceStats{
		TreeType:      "Verkle",
		DataSize:      len(txs),
		ProofCount:    proofCount,
		GenTime:       proofGenTime,
		VerifyTime:    verifyTime,
		AvgGenTime:    proofGenTime / time.Duration(proofCount),
		AvgVerifyTime: verifyTime,
		ProofSize:     int64(len(proofBytes)),
		AvgProofSize:  float64(len(proofBytes)) / float64(proofCount),
		NodeCount:     0, // Verkle证明不提供节点计数
		BlockNumber:   blockNum,
	}

	t.Logf("区块 %d: 生成 %d 个证明，耗时: %v (平均: %v)",
		blockNum, proofCount, proofGenTime, stats.AvgGenTime)
	t.Logf("验证证明，耗时: %v", verifyTime)
	t.Logf("证明大小: %s (平均: %.2f bytes/proof)",
		bytesToReadable(uint64(len(proofBytes))), stats.AvgProofSize)

	return stats
}

// 保留原有的基准测试函数
func BenchmarkMPTProofGeneration(b *testing.B) {
	// 创建内存数据库
	memDB := rawdb.NewMemoryDatabase()
	trieDB := triedb.NewDatabase(memDB, nil)

	// 创建新的MPT树
	tr, err := trie.New(trie.TrieID(common.Hash{}), trieDB)
	if err != nil {
		b.Fatalf("创建新的Trie失败: %v", err)
	}

	// 插入测试数据
	testData := generateTestData(10000)
	for _, data := range testData {
		tr.Update(data.key, data.value)
	}

	// 提交并获取根哈希
	root, _ := tr.Commit(false)

	// 重新打开Trie
	tr, err = trie.New(trie.TrieID(root), trieDB)
	if err != nil {
		b.Fatalf("重新打开Trie失败: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		proofDB := trienode.NewProofSet()
		// 为随机键生成证明
		randomKey := testData[i%len(testData)].key
		if err := tr.Prove(randomKey, proofDB); err != nil {
			b.Fatalf("生成证明失败: %v", err)
		}
	}
}

// BenchmarkVerkleProofGeneration 基准测试Verkle证明生成性能
func BenchmarkVerkleProofGeneration(b *testing.B) {
	// 创建内存数据库
	memDB := rawdb.NewMemoryDatabase()
	cacheConfig := core.DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	trieDB := triedb.NewDatabase(memDB, cacheConfig.TriedbConfig(true))

	// 创建point cache
	pointCache := utils.NewPointCache(1024)

	// 创建新的Verkle树
	vt, err := trie.NewVerkleTrie(common.Hash{}, trieDB, pointCache)
	if err != nil {
		b.Fatalf("创建新的Verkle Trie失败: %v", err)
	}

	// 插入测试数据
	testData := generateTestData(10000)
	testAddr := common.Address{}
	for _, data := range testData {
		if err := vt.UpdateStorage(testAddr, data.key, data.value); err != nil {
			b.Fatalf("更新存储数据失败: %v", err)
		}
	}

	// 提交并获取根哈希
	root, _ := vt.Commit(false)

	// 重新打开Verkle Trie
	vt, err = trie.NewVerkleTrie(root, trieDB, pointCache)
	if err != nil {
		b.Fatalf("重新打开Verkle Trie失败: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 为随机键生成证明
		randomKey := testData[i%len(testData)].key

		// 创建空的post trie用于证明生成
		emptyVerkleTrie, err := trie.NewVerkleTrie(common.Hash{}, trieDB, pointCache)
		if err != nil {
			b.Fatalf("创建空Verkle Trie失败: %v", err)
		}

		_, _, err = vt.Proof(emptyVerkleTrie, [][]byte{randomKey})
		if err != nil {
			b.Fatalf("生成Verkle证明失败: %v", err)
		}
	}
}

// 辅助函数

// TestData 测试数据结构
type TestData struct {
	key   []byte
	value []byte
}

// generateTestData 生成测试数据
func generateTestData(size int) []TestData {
	data := make([]TestData, size)
	for i := 0; i < size; i++ {
		key, value := generateRandomData()
		data[i] = TestData{key: key, value: value}
	}
	return data
}

// generateRandomData 生成随机数据
func generateRandomData() ([]byte, []byte) {
	key := make([]byte, 32)
	value := make([]byte, 32)
	rand.Read(key)
	rand.Read(value)
	return key, value
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// printProofPerformanceComparison 打印证明性能对比
func printProofPerformanceComparison(t *testing.T, stats []ProofPerformanceStats) {
	if len(stats) == 0 {
		return
	}

	t.Log("\n=== 证明性能对比 ===")
	t.Log("树类型 | 区块号 | 证明数量 | 生成时间 | 验证时间 | 平均生成时间 | 平均验证时间 | 证明大小 | 平均证明大小")
	t.Log("-------|--------|----------|----------|----------|--------------|--------------|----------|--------------")

	for _, stat := range stats {
		t.Logf("%s | %d | %d | %v | %v | %v | %v | %s | %.2f bytes",
			stat.TreeType, stat.BlockNumber, stat.ProofCount, stat.GenTime, stat.VerifyTime,
			stat.AvgGenTime, stat.AvgVerifyTime, bytesToReadable(uint64(stat.ProofSize)), stat.AvgProofSize)
	}
}

// saveProofResultsToCSV 保存证明结果到CSV文件
func saveProofResultsToCSV(t *testing.T, filename string, stats []ProofPerformanceStats) {
	file, err := os.Create(filename)
	if err != nil {
		t.Logf("创建CSV文件失败: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入表头
	header := []string{
		"TreeType", "BlockNumber", "DataSize", "ProofCount", "GenTime(ns)", "VerifyTime(ns)",
		"AvgGenTime(ns)", "AvgVerifyTime(ns)", "ProofSize(bytes)", "AvgProofSize(bytes)", "NodeCount",
	}
	if err := writer.Write(header); err != nil {
		t.Logf("写入CSV表头失败: %v", err)
		return
	}

	// 写入数据
	for _, stat := range stats {
		row := []string{
			stat.TreeType,
			strconv.FormatUint(stat.BlockNumber, 10),
			strconv.Itoa(stat.DataSize),
			strconv.Itoa(stat.ProofCount),
			strconv.FormatInt(stat.GenTime.Nanoseconds(), 10),
			strconv.FormatInt(stat.VerifyTime.Nanoseconds(), 10),
			strconv.FormatInt(stat.AvgGenTime.Nanoseconds(), 10),
			strconv.FormatInt(stat.AvgVerifyTime.Nanoseconds(), 10),
			strconv.FormatInt(stat.ProofSize, 10),
			fmt.Sprintf("%.2f", stat.AvgProofSize),
			strconv.FormatUint(stat.NodeCount, 10),
		}
		if err := writer.Write(row); err != nil {
			t.Logf("写入CSV数据失败: %v", err)
			return
		}
	}

	t.Logf("证明性能结果已保存到: %s", filename)
}
