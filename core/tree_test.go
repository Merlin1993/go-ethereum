package core

import (
	"encoding/csv"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb/pebble"
	"github.com/ethereum/go-ethereum/trie/trienode"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	kvsrotage "github.com/ethereum/go-ethereum/kvstorage"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// TestProcessCSVTransactions 测试处理CSV中的交易
func TestProcessCSVTransactions(t *testing.T) {
	endBlock := uint64(100000)

	// 打开CSV文件
	file, err := os.Open("E:\\ethdata\\0to999999_BlockTransaction\\0to999999_BlockTransaction.csv")
	if err != nil {
		t.Fatalf("Failed to open CSV file: %v", err)
	}
	defer file.Close()

	// 创建数据库和处理器
	db := rawdb.NewMemoryDatabase()
	trieDB := triedb.NewDatabase(db, nil)
	engine := ethash.NewFaker()
	gspec := &Genesis{
		Config: params.TestChainConfig,
		Alloc:  GenesisAlloc{},
	}
	genesis := gspec.MustCommit(db, trieDB)

	// 创建区块处理器
	blockchain, _ := NewBlockChain(db, nil, gspec, nil, engine, vm.Config{}, nil)
	defer blockchain.Stop()

	//processor := NewStateProcessor(gspec.Config, blockchain.HeaderChain())

	// 解析CSV数据
	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		t.Fatalf("Failed to read CSV headers: %v", err)
	}
	t.Logf("CSV Headers: %v", headers)

	// 按区块组织交易
	msgsByBlock := make(map[uint64][]*Message)

	// 读取CSV数据并组织交易
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		blockNum := new(big.Int)
		blockNum.SetString(record[0], 10)
		if blockNum.Uint64() > endBlock {
			break
		}

		from := common.HexToAddress(record[1])
		to := common.HexToAddress(record[2])
		value := new(big.Int)
		value.SetString(record[3], 10)

		// 创建消息
		msg := &Message{
			To:         &to,
			From:       from,
			Nonce:      0,
			Value:      value,
			GasLimit:   21000,
			GasPrice:   big.NewInt(1000000000),
			GasFeeCap:  big.NewInt(1000000000),
			GasTipCap:  big.NewInt(1000000000),
			Data:       nil,
			AccessList: nil,
		}

		msgsByBlock[blockNum.Uint64()] = append(msgsByBlock[blockNum.Uint64()], msg)
	}

	// 处理每个区块
	parent := genesis
	for blockNum := uint64(1); blockNum <= endBlock; blockNum++ {
		if len(msgsByBlock[blockNum]) == 0 {
			continue
		}

		// 创建新的区块
		header := &types.Header{
			ParentHash: parent.Hash(),
			Number:     new(big.Int).SetUint64(blockNum),
			GasLimit:   4700000,
			Time:       uint64(blockNum * 15),
			Difficulty: big.NewInt(1),
			BaseFee:    big.NewInt(1000000000),
		}

		// 创建statedb
		statedb, err := state.New(parent.Root(), state.NewDatabase(trieDB, nil))
		if err != nil {
			t.Fatalf("Failed to create state: %v", err)
		}

		bigBalance := new(big.Int).Mul(big.NewInt(1000000), big.NewInt(1e18))
		// 转换为uint256.Int
		balance, overflow := uint256.FromBig(bigBalance)
		if overflow {
			t.Fatalf("Balance overflow")
		}
		// 为所有发送方预分配余额
		for _, msg := range msgsByBlock[blockNum] {
			// 创建一个大数值 1000000 * 10^18
			statedb.SetBalance(msg.From, balance, 0)
		}

		// 处理区块中的所有交易
		processStart := time.Now()
		gp := new(GasPool).AddGas(header.GasLimit)
		var usedGas uint64
		var receipts types.Receipts

		// 创建EVM上下文
		context := NewEVMBlockContext(header, blockchain, nil)
		vmenv := vm.NewEVM(context, statedb, gspec.Config, vm.Config{})

		for _, msg := range msgsByBlock[blockNum] {
			result, err := ApplyMessage(vmenv, msg, gp)
			if err != nil {
				t.Fatalf("Failed to apply message: %v", err)
			}

			usedGas += result.UsedGas

			// 创建收据
			receipt := &types.Receipt{
				Type:              types.LegacyTxType,
				Status:            types.ReceiptStatusSuccessful,
				CumulativeGasUsed: usedGas,
				Logs:              statedb.GetLogs(common.Hash{}, blockNum, common.Hash{}),
				TxHash:            common.Hash{},
				GasUsed:           result.UsedGas,
				BlockNumber:       big.NewInt(int64(blockNum)),
				BlockHash:         common.Hash{},
			}
			receipts = append(receipts, receipt)
		}
		processDuration := time.Since(processStart)

		// 提交状态
		commitStart := time.Now()
		root, err := statedb.Commit(0, true, false)
		commitDuration := time.Since(commitStart)

		if err != nil {
			t.Fatalf("Failed to commit state for block %d: %v", blockNum, err)
		}

		// 更新区块头的状态根
		header.Root = root

		t.Logf("Block %d - Process time: %v, Commit time: %v, Messages: %d",
			blockNum, processDuration, commitDuration, len(msgsByBlock[blockNum]))

		// 创建并保存区块
		block := types.NewBlock(header, nil, nil, nil)
		parent = block
	}
}

func TestProcessBlocks(t *testing.T) {
	TestProcessCSVTransactions(t)
}

// TestCompareTreePerformance 比较Verkle_tree、Trie和KVTree三种数据结构的性能
func TestCompareTreePerformance(t *testing.T) {
	t.Log("这个测试函数是各种测试数据集的总入口")
	t.Log("考虑到测试完整的2亿数据需要较长时间，建议根据需要运行以下测试：")
	t.Log("1. TestSmallDataSet - 测试1万条数据，用于快速检查代码是否正常工作")
	t.Log("2. TestMediumDataSet - 测试100万条数据，提供初步的性能数据")
	t.Log("3. TestLargeDataSet - 测试1000万条数据，获取更有意义的性能统计")
	t.Log("4. TestHugeDataSet - 测试完整的2亿条数据，完全符合原始需求")
	t.Log("请使用 'go test -run=TestSmallDataSet' 等命令单独运行所需的测试")

	// 注释掉实际测试代码，避免意外运行全量测试
	t.Skip("此函数仅作为入口说明，不执行实际测试")

	/*
		// 设置测试参数 - 为了测试可以先用较小的数据量
		const (
			totalEntries      = 1000000 // 初始设置为100万条数据进行测试
			entriesPerCommit  = 3000    // 每3000条数据提交一次
			reportInterval    = 500000    // 每50万条数据报告一次
			checkpointEntries = 100000    // 每10万条数据检查一次内存使用情况
		)

		// 创建存储目录
		os.MkdirAll("E:\\ethdata\\vt", os.ModePerm)
		os.MkdirAll("E:\\ethdata\\mpt", os.ModePerm)
		os.MkdirAll("E:\\ethdata\\kvt", os.ModePerm)

		// 运行KVTree测试
		t.Run("KVTree", func(t *testing.T) {
			testKVTreePerformance(t, "E:\\ethdata\\kvt", totalEntries, entriesPerCommit, reportInterval, checkpointEntries)
		})

		// 运行Trie测试
		t.Run("Trie", func(t *testing.T) {
			testTriePerformance(t, "E:\\ethdata\\mpt", totalEntries, entriesPerCommit, reportInterval, checkpointEntries)
		})
	*/
}

// TestOption 定义测试选项函数类型
type TestOption func(*TestOptions)

// TestOptions 包含测试的配置选项
type TestOptions struct {
	keyValueGenerator     func(index int) ([]byte, []byte)
	verbose               bool
	exportPerformanceData bool
	randomReadCount       int     // 每次提交后随机读取的key数量
	overwriteRatio        float64 // 覆盖旧数据的比例 (0.0-1.0)
}

// 默认测试选项
func defaultTestOptions() *TestOptions {
	return &TestOptions{
		keyValueGenerator: func(index int) ([]byte, []byte) {
			index = index + 1000000000
			// 默认的key和value生成函数
			keyBytes := []byte(fmt.Sprintf("%d", index))
			valueBytes := []byte(fmt.Sprintf("%d", index))
			return keyBytes, valueBytes
		},
		verbose:               false,
		exportPerformanceData: false,
		randomReadCount:       0,   // 默认不进行随机读取
		overwriteRatio:        0.0, // 默认不覆盖旧数据
	}
}

// WithKeyValueGenerator 设置自定义的key-value生成函数
func WithKeyValueGenerator(generator func(index int) ([]byte, []byte)) TestOption {
	return func(opts *TestOptions) {
		opts.keyValueGenerator = generator
	}
}

// WithVerboseOutput 启用详细输出
func WithVerboseOutput(verbose bool) TestOption {
	return func(opts *TestOptions) {
		opts.verbose = verbose
	}
}

// WithPerformanceDataExport 启用性能数据导出
func WithPerformanceDataExport(export bool) TestOption {
	return func(opts *TestOptions) {
		opts.exportPerformanceData = export
	}
}

// WithRandomReads 设置每次提交后随机读取的key数量
func WithRandomReads(count int) TestOption {
	return func(opts *TestOptions) {
		opts.randomReadCount = count
	}
}

// WithOverwriteRatio 设置覆盖旧数据的比例
func WithOverwriteRatio(ratio float64) TestOption {
	return func(opts *TestOptions) {
		if ratio < 0.0 {
			ratio = 0.0
		}
		if ratio > 1.0 {
			ratio = 1.0
		}
		opts.overwriteRatio = ratio
	}
}

// 辅助函数：导出性能数据到CSV
func exportPerformanceData(path string, records []PerformanceRecord) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入标题
	headers := []string{"Entries", "CommitTime(ns)", "UpdateTime(ns)", "RandomReadTime(ns)", "MemoryUsage(bytes)", "DatabaseSize(bytes)", "Timestamp"}
	if err := writer.Write(headers); err != nil {
		return err
	}

	// 写入数据
	for _, record := range records {
		row := []string{
			fmt.Sprintf("%d", record.Entries),
			fmt.Sprintf("%d", record.CommitTime.Nanoseconds()),
			fmt.Sprintf("%d", record.UpdateTime.Nanoseconds()),
			fmt.Sprintf("%d", record.RandomReadTime.Nanoseconds()),
			fmt.Sprintf("%d", record.MemoryUsage),
			fmt.Sprintf("%d", record.DatabaseSize),
			record.Timestamp.Format(time.RFC3339),
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// 辅助函数：返回两个整数中的较大值
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 性能数据记录结构
type PerformanceRecord struct {
	Entries        int
	CommitTime     time.Duration
	UpdateTime     time.Duration
	RandomReadTime time.Duration // 随机读取的时间
	MemoryUsage    uint64
	DatabaseSize   int64
	Timestamp      time.Time
}

// 测试KVTree的性能
func testKVTreePerformance(t *testing.T, dbPath string, totalEntries, entriesPerCommit, reportInterval, checkpointEntries int, options ...TestOption) {
	// 应用测试选项
	opts := defaultTestOptions()
	for _, option := range options {
		option(opts)
	}

	// 创建leveldb数据库
	db, err := openDatabase(dbPath, "kvtree-test")
	if err != nil {
		t.Fatalf("Failed to create KVTree database: %v", err)
	}
	defer db.Close()

	// 准备测量内存使用
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	initialMemory := m.Alloc

	// 创建KVTree数据库
	kvDatabase := kvsrotage.NewKVDatabase(db)

	// 初始化性能统计
	var (
		totalCommitTime     time.Duration
		totalUpdateTime     time.Duration
		totalRandomReadTime time.Duration
		maxCommitTime       time.Duration
		minCommitTime       = time.Hour // 初始化为一个较大的值
		maxUpdateTime       time.Duration
		minUpdateTime       = time.Hour // 初始化为一个较大的值
		commitCount         int
		updateCount         int
		randomReadCount     int
		startTime           = time.Now()
		lastReportTime      = startTime
		totalBytesWritten   int64
	)

	// 创建性能数据记录
	var performanceRecords []PerformanceRecord

	// 创建快照和当前高度
	currentHeight := uint64(1)
	snapshot, _ := kvDatabase.CreateDBSnapshot(common.Hash{}, currentHeight)

	// 存储所有已插入的key，用于随机读取和覆盖
	var allKeys [][]byte
	var allValues [][]byte

	// 处理数据批次
	for i := 0; i < totalEntries; i++ {
		// 决定是插入新数据还是覆盖旧数据
		var keyBytes, valueBytes []byte
		var keyIndex int

		if len(allKeys) > 0 && rand.Float64() < opts.overwriteRatio {
			// 覆盖旧数据
			keyIndex = rand.Intn(len(allKeys))
			keyBytes = allKeys[keyIndex]
			// 生成新的值
			_, valueBytes = opts.keyValueGenerator(i)
		} else {
			// 插入新数据
			keyBytes, valueBytes = opts.keyValueGenerator(i)
			// 保存key和value以便后续随机读取和覆盖
			allKeys = append(allKeys, keyBytes)
			allValues = append(allValues, valueBytes)
		}

		key := common.BytesToHash(keyBytes)
		valueHash := crypto.Keccak256(valueBytes)

		// 测量更新时间
		updateStart := time.Now()
		snapshot.TryUpdate(key.Bytes(), valueHash)
		updateTime := time.Since(updateStart)
		totalUpdateTime += updateTime
		updateCount++

		// 更新最大/最小更新时间
		if updateTime > maxUpdateTime {
			maxUpdateTime = updateTime
		}
		if updateTime < minUpdateTime {
			minUpdateTime = updateTime
		}

		// 每entriesPerCommit条数据提交一次
		if (i+1)%entriesPerCommit == 0 || i == totalEntries-1 {
			// 计算哈希并提交
			commitStart := time.Now()
			hash := snapshot.LedgerHash()
			snapshot.KnotBlock(hash)
			err = kvDatabase.Commit(hash)
			if err != nil {
				t.Fatalf("Failed to commit at entry %d: %v", i, err)
			}
			commitTime := time.Since(commitStart)
			totalCommitTime += commitTime
			commitCount++

			// 更新最大/最小提交时间
			if commitTime > maxCommitTime {
				maxCommitTime = commitTime
			}
			if commitTime < minCommitTime {
				minCommitTime = commitTime
			}

			// 重新创建快照
			currentHeight++
			snapshot, _ = kvDatabase.CreateDBSnapshot(hash, currentHeight)

			// 执行随机读取操作
			if opts.randomReadCount > 0 && len(allKeys) > 0 {
				randomReadStart := time.Now()

				// 确定要读取的数量（不超过已有的key数量）
				readCount := opts.randomReadCount
				if readCount > len(allKeys) {
					readCount = len(allKeys)
				}

				// 随机读取指定数量的key
				for j := 0; j < readCount; j++ {
					// 随机选择一个key
					idx := rand.Intn(len(allKeys))
					key := common.BytesToHash(allKeys[idx])

					// 读取该key的值
					snapshot.TryGet(key.Bytes())
				}

				randomReadTime := time.Since(randomReadStart)
				totalRandomReadTime += randomReadTime
				randomReadCount++
			}
		}

		// 每checkpointEntries条数据检查一次内存使用
		if (i+1)%checkpointEntries == 0 {
			runtime.GC() // 触发GC以获得更准确的内存使用数据
			runtime.ReadMemStats(&m)

			// 获取数据库大小
			dbSize, _ := getDirSize(dbPath)
			totalBytesWritten = dbSize

			// 记录性能数据点
			performanceRecords = append(performanceRecords, PerformanceRecord{
				Entries:        i + 1,
				CommitTime:     totalCommitTime / time.Duration(maxInt(commitCount, 1)),
				UpdateTime:     totalUpdateTime / time.Duration(maxInt(updateCount, 1)),
				RandomReadTime: totalRandomReadTime / time.Duration(maxInt(randomReadCount, 1)),
				MemoryUsage:    m.Alloc - initialMemory,
				DatabaseSize:   dbSize,
				Timestamp:      time.Now(),
			})
		}

		// 每reportInterval条数据报告一次
		if (i+1)%reportInterval == 0 {
			// 计算统计信息
			elapsed := time.Since(startTime)
			entriesProcessed := i + 1
			avgCommitTime := totalCommitTime / time.Duration(maxInt(commitCount, 1))
			avgUpdateTime := totalUpdateTime / time.Duration(maxInt(updateCount, 1))
			avgRandomReadTime := totalRandomReadTime / time.Duration(maxInt(randomReadCount, 1))

			// 计算吞吐量
			throughput := float64(entriesProcessed) / elapsed.Seconds()
			recentThroughput := float64(reportInterval) / time.Since(lastReportTime).Seconds()
			lastReportTime = time.Now()

			// 获取数据库大小
			dbSize, _ := getDirSize(dbPath)
			totalBytesWritten = dbSize

			// 获取内存使用
			currentMemory := m.Alloc
			memoryUsage := currentMemory - initialMemory

			// 报告性能
			t.Logf("KVTree 性能报告 (处理 %d 条目):\n", entriesProcessed)
			t.Logf("  总耗时: %v\n", elapsed)
			t.Logf("  平均提交时间: %v (最小: %v, 最大: %v)\n", avgCommitTime, minCommitTime, maxCommitTime)
			t.Logf("  平均更新时间: %v (最小: %v, 最大: %v)\n", avgUpdateTime, minUpdateTime, maxUpdateTime)
			if randomReadCount > 0 {
				t.Logf("  平均随机读取时间: %v\n", avgRandomReadTime)
			}
			t.Logf("  数据库大小: %d MB (%.2f bytes/entry)\n", dbSize/(1024*1024), float64(dbSize)/float64(entriesProcessed))
			t.Logf("  内存使用 (估计): %d MB\n", memoryUsage/(1024*1024))
			t.Logf("  总体吞吐量: %.2f entries/sec\n", throughput)
			t.Logf("  当前吞吐量: %.2f entries/sec\n", recentThroughput)
			t.Logf("  覆盖率: %.2f%%\n", opts.overwriteRatio*100)
			t.Logf("  每次提交后随机读取: %d 次\n", opts.randomReadCount)

			// 如果启用了详细模式，输出更多信息
			if opts.verbose {
				t.Logf("  GC统计: 次数=%d, 暂停总时间=%v\n", m.NumGC, time.Duration(m.PauseTotalNs))
				t.Logf("  堆统计: 堆对象=%d, 堆释放=%d\n", m.HeapObjects, m.Frees)
			}
		}
	}

	// 测试结束，输出总结
	totalTime := time.Since(startTime)
	t.Logf("\nKVTree 性能测试总结:")
	t.Logf("  总条目数: %d", totalEntries)
	t.Logf("  总耗时: %v", totalTime)
	t.Logf("  平均吞吐量: %.2f entries/sec", float64(totalEntries)/totalTime.Seconds())
	t.Logf("  数据库最终大小: %.2f MB", float64(totalBytesWritten)/(1024*1024))
	t.Logf("  每条目平均大小: %.2f bytes", float64(totalBytesWritten)/float64(totalEntries))
	t.Logf("  覆盖率: %.2f%%", opts.overwriteRatio*100)
	t.Logf("  每次提交后随机读取: %d 次", opts.randomReadCount)

	// 如果启用了导出性能数据，将数据写入CSV
	if opts.exportPerformanceData && len(performanceRecords) > 0 {
		exportPath := dbPath + "_performance.csv"
		exportPerformanceData(exportPath, performanceRecords)
		t.Logf("  性能数据已导出到: %s", exportPath)
	}
}

// 测试Trie的性能
func testTriePerformance(t *testing.T, dbPath string, totalEntries, entriesPerCommit, reportInterval, checkpointEntries int, options ...TestOption) {
	// 应用测试选项
	opts := defaultTestOptions()
	for _, option := range options {
		option(opts)
	}

	// 创建leveldb数据库
	db, err := openDatabase(dbPath, "mpt-test")
	if err != nil {
		t.Fatalf("Failed to create Trie database: %v", err)
	}
	defer db.Close()

	// 准备测量内存使用
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	initialMemory := m.Alloc

	// 创建trie数据库
	trieDB := triedb.NewDatabase(db, nil)

	// 初始化性能统计
	var (
		totalCommitTime     time.Duration
		totalUpdateTime     time.Duration
		totalRandomReadTime time.Duration
		maxCommitTime       time.Duration
		minCommitTime       = time.Hour // 初始化为一个较大的值
		maxUpdateTime       time.Duration
		minUpdateTime       = time.Hour // 初始化为一个较大的值
		commitCount         int
		updateCount         int
		randomReadCount     int
		startTime           = time.Now()
		lastReportTime      = startTime
		totalBytesWritten   int64
	)

	// 创建性能数据记录
	var performanceRecords []PerformanceRecord

	EndRoot := common.Hash{}

	// 创建trie
	tr, err := trie.New(trie.TrieID(common.HexToHash("0xbea25524214a66e2356c5efebf63e5199d0d551fa61cf90b20a90e013e5349f7")), trieDB)
	if err != nil {
		t.Fatalf("Failed to new tire %v", err)
	}
	//tr := trie.NewEmpty(trieDB)

	// 存储所有已插入的key，用于随机读取和覆盖
	var allKeys [][]byte
	var allValues [][]byte

	// 处理数据批次
	for i := 0; i < totalEntries; i++ {
		// 决定是插入新数据还是覆盖旧数据
		var keyBytes, valueBytes []byte

		if len(allKeys) > 0 && rand.Float64() < opts.overwriteRatio {
			// 覆盖旧数据
			keyIndex := rand.Intn(len(allKeys))
			keyBytes = allKeys[keyIndex]
			// 生成新的值
			_, valueBytes = opts.keyValueGenerator(i)
		} else {
			// 插入新数据
			keyBytes, valueBytes = opts.keyValueGenerator(i)
			// 保存key和value以便后续随机读取和覆盖
			allKeys = append(allKeys, keyBytes)
			allValues = append(allValues, valueBytes)
		}

		key := common.BytesToHash(keyBytes)
		valueHash := crypto.Keccak256(valueBytes)

		// 测量更新时间
		updateStart := time.Now()
		tr.Update(key.Bytes(), valueHash)
		updateTime := time.Since(updateStart)
		totalUpdateTime += updateTime
		updateCount++

		// 更新最大/最小更新时间
		if updateTime > maxUpdateTime {
			maxUpdateTime = updateTime
		}
		if updateTime < minUpdateTime {
			minUpdateTime = updateTime
		}

		// 每entriesPerCommit条数据提交一次
		if (i+1)%entriesPerCommit == 0 || i == totalEntries-1 {
			// 计算哈希并提交
			commitStart := time.Now()
			root, nodes := tr.Commit(false)
			EndRoot = root

			// 批量写入数据库
			if err := trieDB.Update(root, common.Hash{}, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
				t.Fatalf("Failed to update trie db at entry %d: %v", i, err)
			}
			if err := trieDB.Commit(root, false); err != nil {
				t.Fatalf("Failed to commit trie db at entry %d: %v", i, err)
			}

			commitTime := time.Since(commitStart)
			totalCommitTime += commitTime
			commitCount++

			// 更新最大/最小提交时间
			if commitTime > maxCommitTime {
				maxCommitTime = commitTime
			}
			if commitTime < minCommitTime {
				minCommitTime = commitTime
			}

			// 创建新的trie以继续操作
			tr, err = trie.New(trie.TrieID(root), trieDB)
			if err != nil {
				t.Fatalf("Failed to create new trie at entry %d: %v", i, err)
			}

			// 执行随机读取操作
			if opts.randomReadCount > 0 && len(allKeys) > 0 {
				randomReadStart := time.Now()

				// 确定要读取的数量（不超过已有的key数量）
				readCount := opts.randomReadCount
				if readCount > len(allKeys) {
					readCount = len(allKeys)
				}

				// 随机读取指定数量的key
				for j := 0; j < readCount; j++ {
					// 随机选择一个key
					idx := rand.Intn(len(allKeys))
					key := common.BytesToHash(allKeys[idx])

					// 读取该key的值
					tr.Get(key.Bytes())
				}

				randomReadTime := time.Since(randomReadStart)
				totalRandomReadTime += randomReadTime
				randomReadCount++
			}
		}

		// 每checkpointEntries条数据检查一次内存使用
		if (i+1)%checkpointEntries == 0 {
			runtime.GC() // 触发GC以获得更准确的内存使用数据
			runtime.ReadMemStats(&m)

			// 获取数据库大小
			dbSize, _ := getDirSize(dbPath)
			totalBytesWritten = dbSize

			// 记录性能数据点
			performanceRecords = append(performanceRecords, PerformanceRecord{
				Entries:        i + 1,
				CommitTime:     totalCommitTime / time.Duration(maxInt(commitCount, 1)),
				UpdateTime:     totalUpdateTime / time.Duration(maxInt(updateCount, 1)),
				RandomReadTime: totalRandomReadTime / time.Duration(maxInt(randomReadCount, 1)),
				MemoryUsage:    m.Alloc - initialMemory,
				DatabaseSize:   dbSize,
				Timestamp:      time.Now(),
			})
		}

		// 每reportInterval条数据报告一次
		if (i+1)%reportInterval == 0 {
			// 计算统计信息
			elapsed := time.Since(startTime)
			entriesProcessed := i + 1
			avgCommitTime := totalCommitTime / time.Duration(maxInt(commitCount, 1))
			avgUpdateTime := totalUpdateTime / time.Duration(maxInt(updateCount, 1))
			avgRandomReadTime := totalRandomReadTime / time.Duration(maxInt(randomReadCount, 1))

			// 计算吞吐量
			throughput := float64(entriesProcessed) / elapsed.Seconds()
			recentThroughput := float64(reportInterval) / time.Since(lastReportTime).Seconds()
			lastReportTime = time.Now()

			// 获取数据库大小
			dbSize, _ := getDirSize(dbPath)
			totalBytesWritten = dbSize

			// 获取内存使用
			currentMemory := m.Alloc
			memoryUsage := currentMemory - initialMemory

			// 报告性能
			t.Logf("Trie Root : %v \n", EndRoot.String())
			t.Logf("Trie 性能报告 (处理 %d 条目):\n", entriesProcessed)
			t.Logf("  总耗时: %v\n", elapsed)
			t.Logf("  平均提交时间: %v (最小: %v, 最大: %v)\n", avgCommitTime, minCommitTime, maxCommitTime)
			t.Logf("  平均更新时间: %v (最小: %v, 最大: %v)\n", avgUpdateTime, minUpdateTime, maxUpdateTime)
			if randomReadCount > 0 {
				t.Logf("  平均随机读取时间: %v\n", avgRandomReadTime)
			}
			t.Logf("  数据库大小: %d MB (%.2f bytes/entry)\n", dbSize/(1024*1024), float64(dbSize)/float64(entriesProcessed))
			t.Logf("  内存使用 (估计): %d MB\n", memoryUsage/(1024*1024))
			t.Logf("  总体吞吐量: %.2f entries/sec\n", throughput)
			t.Logf("  当前吞吐量: %.2f entries/sec\n", recentThroughput)
			t.Logf("  覆盖率: %.2f%%\n", opts.overwriteRatio*100)
			t.Logf("  每次提交后随机读取: %d 次\n", opts.randomReadCount)

			// 如果启用了详细模式，输出更多信息
			if opts.verbose {
				t.Logf("  GC统计: 次数=%d, 暂停总时间=%v\n", m.NumGC, time.Duration(m.PauseTotalNs))
				t.Logf("  堆统计: 堆对象=%d, 堆释放=%d\n", m.HeapObjects, m.Frees)
			}
		}
	}

	// 测试结束，输出总结
	totalTime := time.Since(startTime)
	t.Logf("\nTrie 性能测试总结:")
	t.Logf("  总条目数: %d", totalEntries)
	t.Logf("  总耗时: %v", totalTime)
	t.Logf("  平均吞吐量: %.2f entries/sec", float64(totalEntries)/totalTime.Seconds())
	t.Logf("  数据库最终大小: %.2f MB", float64(totalBytesWritten)/(1024*1024))
	t.Logf("  每条目平均大小: %.2f bytes", float64(totalBytesWritten)/float64(totalEntries))
	t.Logf("  覆盖率: %.2f%%", opts.overwriteRatio*100)
	t.Logf("  每次提交后随机读取: %d 次", opts.randomReadCount)

	// 如果启用了导出性能数据，将数据写入CSV
	if opts.exportPerformanceData && len(performanceRecords) > 0 {
		exportPath := dbPath + "_performance.csv"
		exportPerformanceData(exportPath, performanceRecords)
		t.Logf("  性能数据已导出到: %s", exportPath)
	}
}

// 测试VerkleTree的性能
func testVerkleTreePerformance(t *testing.T, dbPath string, totalEntries, entriesPerCommit, reportInterval, checkpointEntries int, options ...TestOption) {
	// 应用测试选项
	opts := defaultTestOptions()
	for _, option := range options {
		option(opts)
	}

	// 创建leveldb数据库
	db, err := openDatabase(dbPath, "verkle-test")
	if err != nil {
		t.Fatalf("Failed to create VerkleTree database: %v", err)
	}
	defer db.Close()

	// 准备测量内存使用
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	initialMemory := m.Alloc

	// 创建trie数据库
	trieDB := triedb.NewDatabase(db, nil)

	// 初始化性能统计
	var (
		totalCommitTime     time.Duration
		totalUpdateTime     time.Duration
		totalRandomReadTime time.Duration
		maxCommitTime       time.Duration
		minCommitTime       = time.Hour // 初始化为一个较大的值
		maxUpdateTime       time.Duration
		minUpdateTime       = time.Hour // 初始化为一个较大的值
		commitCount         int
		updateCount         int
		randomReadCount     int
		startTime           = time.Now()
		lastReportTime      = startTime
		totalBytesWritten   int64
	)

	// 创建性能数据记录
	var performanceRecords []PerformanceRecord

	// 创建point cache用于verkle tree
	cache := utils.NewPointCache(1024)

	// 创建verkle trie
	vt, err := trie.NewVerkleTrie(common.Hash{}, trieDB, cache)
	if err != nil {
		t.Fatalf("Failed to create verkle trie: %v", err)
	}

	// 存储所有已插入的key，用于随机读取和覆盖
	var allKeys [][]byte
	var allValues [][]byte

	// 处理数据批次
	for i := 0; i < totalEntries; i++ {
		// 决定是插入新数据还是覆盖旧数据
		var keyBytes, valueBytes []byte

		if len(allKeys) > 0 && rand.Float64() < opts.overwriteRatio {
			// 覆盖旧数据
			keyIndex := rand.Intn(len(allKeys))
			keyBytes = allKeys[keyIndex]
			// 生成新的值
			_, valueBytes = opts.keyValueGenerator(i)
		} else {
			// 插入新数据
			keyBytes, valueBytes = opts.keyValueGenerator(i)
			// 保存key和value以便后续随机读取和覆盖
			allKeys = append(allKeys, keyBytes)
			allValues = append(allValues, valueBytes)
		}

		key := common.BytesToHash(keyBytes)
		valueHash := crypto.Keccak256(valueBytes)

		// 测量更新时间
		updateStart := time.Now()
		// Verkle trie的更新与其他类型不同，这里使用UpdateStorage
		// 使用固定的地址，避免地址生成的开销
		addr := common.Address{}
		err = vt.UpdateStorage(addr, key.Bytes(), valueHash)
		if err != nil {
			t.Fatalf("Failed to update verkle tree at entry %d: %v", i, err)
		}
		updateTime := time.Since(updateStart)
		totalUpdateTime += updateTime
		updateCount++

		// 更新最大/最小更新时间
		if updateTime > maxUpdateTime {
			maxUpdateTime = updateTime
		}
		if updateTime < minUpdateTime {
			minUpdateTime = updateTime
		}

		// 每entriesPerCommit条数据提交一次
		if (i+1)%entriesPerCommit == 0 || i == totalEntries-1 {
			// 计算哈希并提交
			commitStart := time.Now()
			root, nodes := vt.Commit(false)
			// protect two maps below
			mn := trienode.NewMergedNodeSet()
			mn.Merge(nodes)

			// 批量写入数据库
			if err := trieDB.Update(root, common.Hash{}, 0, mn, nil); err != nil {
				t.Fatalf("Failed to update verkle db at entry %d: %v", i, err)
			}
			if err := trieDB.Commit(root, false); err != nil {
				t.Fatalf("Failed to commit verkle db at entry %d: %v", i, err)
			}

			commitTime := time.Since(commitStart)
			totalCommitTime += commitTime
			commitCount++

			// 更新最大/最小提交时间
			if commitTime > maxCommitTime {
				maxCommitTime = commitTime
			}
			if commitTime < minCommitTime {
				minCommitTime = commitTime
			}

			// 创建新的verkle trie以继续操作
			vt, err = trie.NewVerkleTrie(root, trieDB, cache)
			if err != nil {
				t.Fatalf("Failed to create new verkle trie at entry %d: %v", i, err)
			}

			// 执行随机读取操作
			if opts.randomReadCount > 0 && len(allKeys) > 0 {
				randomReadStart := time.Now()

				// 确定要读取的数量（不超过已有的key数量）
				readCount := opts.randomReadCount
				if readCount > len(allKeys) {
					readCount = len(allKeys)
				}

				// 随机读取指定数量的key
				for j := 0; j < readCount; j++ {
					// 随机选择一个key
					idx := rand.Intn(len(allKeys))
					key := common.BytesToHash(allKeys[idx])

					// 读取该key的值
					vt.GetStorage(addr, key.Bytes())
				}

				randomReadTime := time.Since(randomReadStart)
				totalRandomReadTime += randomReadTime
				randomReadCount++
			}
		}

		// 每checkpointEntries条数据检查一次内存使用
		if (i+1)%checkpointEntries == 0 {
			runtime.GC() // 触发GC以获得更准确的内存使用数据
			runtime.ReadMemStats(&m)

			// 获取数据库大小
			dbSize, _ := getDirSize(dbPath)
			totalBytesWritten = dbSize

			// 记录性能数据点
			performanceRecords = append(performanceRecords, PerformanceRecord{
				Entries:        i + 1,
				CommitTime:     totalCommitTime / time.Duration(maxInt(commitCount, 1)),
				UpdateTime:     totalUpdateTime / time.Duration(maxInt(updateCount, 1)),
				RandomReadTime: totalRandomReadTime / time.Duration(maxInt(randomReadCount, 1)),
				MemoryUsage:    m.Alloc - initialMemory,
				DatabaseSize:   dbSize,
				Timestamp:      time.Now(),
			})
		}

		// 每reportInterval条数据报告一次
		if (i+1)%reportInterval == 0 {
			// 计算统计信息
			elapsed := time.Since(startTime)
			entriesProcessed := i + 1
			avgCommitTime := totalCommitTime / time.Duration(maxInt(commitCount, 1))
			avgUpdateTime := totalUpdateTime / time.Duration(maxInt(updateCount, 1))
			avgRandomReadTime := totalRandomReadTime / time.Duration(maxInt(randomReadCount, 1))

			// 计算吞吐量
			throughput := float64(entriesProcessed) / elapsed.Seconds()
			recentThroughput := float64(reportInterval) / time.Since(lastReportTime).Seconds()
			lastReportTime = time.Now()

			// 获取数据库大小
			dbSize, _ := getDirSize(dbPath)
			totalBytesWritten = dbSize

			// 获取内存使用
			currentMemory := m.Alloc
			memoryUsage := currentMemory - initialMemory

			// 报告性能
			t.Logf("VerkleTree 性能报告 (处理 %d 条目):\n", entriesProcessed)
			t.Logf("  总耗时: %v\n", elapsed)
			t.Logf("  平均提交时间: %v (最小: %v, 最大: %v)\n", avgCommitTime, minCommitTime, maxCommitTime)
			t.Logf("  平均更新时间: %v (最小: %v, 最大: %v)\n", avgUpdateTime, minUpdateTime, maxUpdateTime)
			if randomReadCount > 0 {
				t.Logf("  平均随机读取时间: %v\n", avgRandomReadTime)
			}
			t.Logf("  数据库大小: %d MB (%.2f bytes/entry)\n", dbSize/(1024*1024), float64(dbSize)/float64(entriesProcessed))
			t.Logf("  内存使用 (估计): %d MB\n", memoryUsage/(1024*1024))
			t.Logf("  总体吞吐量: %.2f entries/sec\n", throughput)
			t.Logf("  当前吞吐量: %.2f entries/sec\n", recentThroughput)
			t.Logf("  覆盖率: %.2f%%\n", opts.overwriteRatio*100)
			t.Logf("  每次提交后随机读取: %d 次\n", opts.randomReadCount)

			// 如果启用了详细模式，输出更多信息
			if opts.verbose {
				t.Logf("  GC统计: 次数=%d, 暂停总时间=%v\n", m.NumGC, time.Duration(m.PauseTotalNs))
				t.Logf("  堆统计: 堆对象=%d, 堆释放=%d\n", m.HeapObjects, m.Frees)
			}
		}
	}

	// 测试结束，输出总结
	totalTime := time.Since(startTime)
	t.Logf("\nVerkleTree 性能测试总结:")
	t.Logf("  总条目数: %d", totalEntries)
	t.Logf("  总耗时: %v", totalTime)
	t.Logf("  平均吞吐量: %.2f entries/sec", float64(totalEntries)/totalTime.Seconds())
	t.Logf("  数据库最终大小: %.2f MB", float64(totalBytesWritten)/(1024*1024))
	t.Logf("  每条目平均大小: %.2f bytes", float64(totalBytesWritten)/float64(totalEntries))
	t.Logf("  覆盖率: %.2f%%", opts.overwriteRatio*100)
	t.Logf("  每次提交后随机读取: %d 次", opts.randomReadCount)

	// 如果启用了导出性能数据，将数据写入CSV
	if opts.exportPerformanceData && len(performanceRecords) > 0 {
		exportPath := dbPath + "_performance.csv"
		exportPerformanceData(exportPath, performanceRecords)
		t.Logf("  性能数据已导出到: %s", exportPath)
	}
}

// 使用openKeyValueDatabase方法打开持久化数据库
func openDatabase(path string, namespace string) (ethdb.Database, error) {
	// 使用rawdb包中的方法直接打开leveldb数据库
	//db, err := leveldb.New(path, 256, 256, namespace, false)
	//if err != nil {
	//	return nil, err
	//}
	//return rawdb.NewDatabase(db), nil

	db, err := pebble.New(path, 256, 256, namespace, false)
	if err != nil {
		return nil, err
	}
	return rawdb.NewDatabase(db), nil
}

// 获取目录大小（以字节为单位）
func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func TestCompareTreeStructures(t *testing.T) {
	TestCompareTreePerformance(t)
}

// TestSmallDataSet 测试小数据集（1万条数据）
func TestSmallDataSet(t *testing.T) {
	const (
		totalEntries      = 10000 // 1万条数据
		entriesPerCommit  = 3000  // 每3000条提交一次
		reportInterval    = 5000  // 每5000条报告一次
		checkpointEntries = 1000  // 每1000条检查内存
	)

	// 创建存储目录
	os.MkdirAll("E:\\ethdata\\ztree\\mpt", os.ModePerm)
	os.MkdirAll("E:\\ethdata\\ztree\\kvt", os.ModePerm)
	os.MkdirAll("E:\\ethdata\\ztree\\vt", os.ModePerm) // 添加VerkleTree的存储目录

	// 设置测试选项
	options := []TestOption{
		WithVerboseOutput(true),
		WithPerformanceDataExport(true),
		WithRandomReads(3000),
		WithOverwriteRatio(0.5),
	}

	// 运行测试
	t.Run("KVTree-Small", func(t *testing.T) {
		//testKVTreePerformance(t, "E:\\ethdata\\ztree\\kvt\\small", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})

	t.Run("Trie-Small", func(t *testing.T) {
		testTriePerformance(t, "E:\\ethdata\\ztree\\mpt\\small", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})

	t.Run("VerkleTree-Small", func(t *testing.T) {
		//testVerkleTreePerformance(t, "E:\\ethdata\\ztree\\vt\\small", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})
}

// TestMediumDataSet 测试中等数据集（100万条数据）
func TestMediumDataSet(t *testing.T) {
	const (
		totalEntries      = 1000000 // 100万条数据
		entriesPerCommit  = 5000    // 每5000条提交一次
		reportInterval    = 500000  // 每20万条报告一次
		checkpointEntries = 50000   // 每5万条检查内存
	)

	// 创建存储目录
	os.MkdirAll("E:\\ethdata\\ztree\\mpt", os.ModePerm)
	os.MkdirAll("E:\\ethdata\\ztree\\kvt", os.ModePerm)
	os.MkdirAll("E:\\ethdata\\ztree\\vt", os.ModePerm) // 添加VerkleTree的存储目录

	// 设置测试选项
	options := []TestOption{
		WithVerboseOutput(true),
		WithPerformanceDataExport(true),
		WithOverwriteRatio(0),
	}

	// 运行测试
	t.Run("KVTree-Medium", func(t *testing.T) {
		testKVTreePerformance(t, "E:\\ethdata\\ztree\\kvt\\medium", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})

	t.Run("Trie-Medium", func(t *testing.T) {
		//testTriePerformance(t, "E:\\ethdata\\ztree\\mpt\\medium", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})

	t.Run("VerkleTree-Medium", func(t *testing.T) {
		// 注释掉以避免长时间运行，需要时可以取消注释
		//testVerkleTreePerformance(t, "E:\\ethdata\\ztree\\vt\\medium", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})
}

// TestLargeDataSet 测试大数据集（1000万条数据）
func TestLargeDataSet(t *testing.T) {
	const (
		totalEntries      = 10000000 // 1000万条数据
		entriesPerCommit  = 5000     // 每5000条提交一次
		reportInterval    = 2000000  // 每200万条报告一次
		checkpointEntries = 500000   // 每50万条检查内存
	)

	// 创建存储目录
	os.MkdirAll("E:\\ethdata\\ztree\\mpt", os.ModePerm)
	os.MkdirAll("E:\\ethdata\\ztree\\kvt", os.ModePerm)
	os.MkdirAll("E:\\ethdata\\ztree\\vt", os.ModePerm) // 添加VerkleTree的存储目录

	// 设置测试选项
	options := []TestOption{
		WithVerboseOutput(true),
		WithPerformanceDataExport(true),
	}

	// 运行测试
	t.Run("KVTree-Large", func(t *testing.T) {
		testKVTreePerformance(t, "E:\\ethdata\\ztree\\kvt\\large", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})

	t.Run("Trie-Large", func(t *testing.T) {
		testTriePerformance(t, "E:\\ethdata\\ztree\\mpt\\large", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})

	t.Run("VerkleTree-Large", func(t *testing.T) {
		//testVerkleTreePerformance(t, "E:\\ethdata\\ztree\\vt\\large", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})
}

// TestHugeDataSet 测试超大数据集（2亿条数据，原需求）
func TestHugeDataSet(t *testing.T) {
	const (
		totalEntries      = 6000 // 2亿条数据
		entriesPerCommit  = 3000 // 每3000条提交一次
		reportInterval    = 3000 // 每2000万条报告一次
		checkpointEntries = 3000 // 每100万条检查内存
	)

	// 创建存储目录
	os.MkdirAll("E:\\ethdata\\ztree\\mpt", os.ModePerm)
	//os.MkdirAll("E:\\ethdata\\ztree\\kvt", os.ModePerm)
	//os.MkdirAll("E:\\ethdata\\ztree\\vt", os.ModePerm) // 添加VerkleTree的存储目录

	// 设置测试选项
	options := []TestOption{
		WithVerboseOutput(true),
		WithPerformanceDataExport(true),
		WithOverwriteRatio(0.3),
	}

	// 运行测试
	t.Run("KVTree-Huge", func(t *testing.T) {
		//testKVTreePerformance(t, "E:\\ethdata\\ztree\\kvt\\huge", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})

	t.Run("Trie-Huge", func(t *testing.T) {
		testTriePerformance(t, "E:\\ethdata\\ztree\\mpt\\huge", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})

	t.Run("VerkleTree-Huge", func(t *testing.T) {
		//testVerkleTreePerformance(t, "E:\\ethdata\\ztree\\vt\\huge", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})
}

// TestCompareAllTrees 测试所有三种树结构
func TestCompareAllTrees(t *testing.T) {
	const (
		totalEntries      = 10000 // 1万条数据
		entriesPerCommit  = 3000  // 每3000条提交一次
		reportInterval    = 5000  // 每5000条报告一次
		checkpointEntries = 1000  // 每1000条检查内存
	)

	// 创建存储目录
	os.MkdirAll("E:\\ethdata\\ztree\\mpt", os.ModePerm)
	os.MkdirAll("E:\\ethdata\\ztree\\kvt", os.ModePerm)
	os.MkdirAll("E:\\ethdata\\ztree\\vt", os.ModePerm)

	// 设置测试选项
	options := []TestOption{
		WithVerboseOutput(true),
		WithPerformanceDataExport(true),
		WithKeyValueGenerator(func(index int) ([]byte, []byte) {
			// 自定义的key和value生成函数，生成更随机的数据
			keyBytes := crypto.Keccak256([]byte(fmt.Sprintf("key-%d", index)))
			valueBytes := crypto.Keccak256([]byte(fmt.Sprintf("value-%d", index)))
			return keyBytes, valueBytes
		}),
	}

	// 运行测试
	t.Run("KVTree-All", func(t *testing.T) {
		testKVTreePerformance(t, "E:\\ethdata\\ztree\\kvt\\all", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})

	t.Run("Trie-All", func(t *testing.T) {
		testTriePerformance(t, "E:\\ethdata\\ztree\\mpt\\all", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})

	t.Run("VerkleTree-All", func(t *testing.T) {
		testVerkleTreePerformance(t, "E:\\ethdata\\ztree\\vt\\all", totalEntries, entriesPerCommit, reportInterval, checkpointEntries, options...)
	})
}
