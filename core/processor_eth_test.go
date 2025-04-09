package core

import (
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/tracing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

// StateAccessCounter 用于记录状态访问信息
type StateAccessCounter struct {
	ReadStates     map[string]bool            // 记录读取的状态
	WriteStates    map[string]bool            // 记录写入的状态
	AccountReads   map[string]bool            // 记录账户读取(只记录一次)
	BlockNum       uint64                     // 当前区块号
	RecentAccess   map[uint64]map[string]bool // 按区块号记录访问的状态
	MaxHistorySize uint64                     // 最大历史记录数量
}

// NewStateAccessCounter 创建一个新的状态访问计数器
func NewStateAccessCounter(maxHistorySize uint64) *StateAccessCounter {
	return &StateAccessCounter{
		ReadStates:     make(map[string]bool),
		WriteStates:    make(map[string]bool),
		AccountReads:   make(map[string]bool),
		RecentAccess:   make(map[uint64]map[string]bool),
		MaxHistorySize: maxHistorySize,
	}
}

// RecordStateRead 记录状态读取
func (c *StateAccessCounter) RecordStateRead(key string) {
	c.ReadStates[key] = true
	if c.RecentAccess[c.BlockNum] == nil {
		c.RecentAccess[c.BlockNum] = make(map[string]bool)
	}
	c.RecentAccess[c.BlockNum][key] = true
}

// RecordAccountRead 记录账户基础信息读取(只记录一次)
func (c *StateAccessCounter) RecordAccountRead(addr string) {
	// 如果账户第一次被读取，记录它
	if !c.AccountReads[addr] {
		c.AccountReads[addr] = true
		c.ReadStates[addr] = true
		if c.RecentAccess[c.BlockNum] == nil {
			c.RecentAccess[c.BlockNum] = make(map[string]bool)
		}
		c.RecentAccess[c.BlockNum][addr] = true
	}
}

// RecordStateWrite 记录状态写入
func (c *StateAccessCounter) RecordStateWrite(key string) {
	// 写入前也会读取
	c.ReadStates[key] = true
	c.WriteStates[key] = true
	if c.RecentAccess[c.BlockNum] == nil {
		c.RecentAccess[c.BlockNum] = make(map[string]bool)
	}
	c.RecentAccess[c.BlockNum][key] = true
}

// NextBlock 进入下一个区块，清除旧的状态记录
func (c *StateAccessCounter) NextBlock(blockNum uint64) {
	c.BlockNum = blockNum
	c.ReadStates = make(map[string]bool)
	c.WriteStates = make(map[string]bool)
	c.AccountReads = make(map[string]bool)

	// 删除历史过久的记录
	for b := range c.RecentAccess {
		if b <= blockNum-c.MaxHistorySize {
			delete(c.RecentAccess, b)
		}
	}
}

// GetRecentAccessStats 获取最近N个区块中访问过的状态数量
func (c *StateAccessCounter) GetRecentAccessStats(blockNum uint64, n uint64) int {
	uniqueStates := make(map[string]bool)
	startBlock := blockNum - n
	if startBlock > blockNum { // 避免溢出
		startBlock = 0
	}

	for b, states := range c.RecentAccess {
		if b >= startBlock && b <= blockNum {
			for state := range states {
				uniqueStates[state] = true
			}
		}
	}

	return len(uniqueStates)
}

// GetCacheHitRate 计算当前区块状态访问的缓存命中率
// 即当前区块访问的状态中，有多少已经在最近N个区块中被访问过
func (c *StateAccessCounter) GetCacheHitRate(n uint64) float64 {
	// 如果当前区块没有访问状态，返回0
	currentBlockStates := c.RecentAccess[c.BlockNum]
	if len(currentBlockStates) == 0 {
		return 0
	}

	// 构建历史访问状态集合(不包括当前区块)
	historicalStates := make(map[string]bool)
	startBlock := c.BlockNum - n
	if startBlock > c.BlockNum { // 避免溢出
		startBlock = 0
	}

	for b, states := range c.RecentAccess {
		if b >= startBlock && b < c.BlockNum { // 不包括当前区块
			for state := range states {
				historicalStates[state] = true
			}
		}
	}

	// 计算当前区块中有多少状态命中了历史访问
	var hitCount int
	for state := range currentBlockStates {
		if historicalStates[state] {
			hitCount++
		}
	}

	// 计算命中率
	return float64(hitCount) / float64(len(currentBlockStates))
}

// 自定义的StateDB包装器，用于拦截状态访问
type CountingStateDB struct {
	*state.StateDB
	counter *StateAccessCounter
}

// GetState 重写GetState方法，增加计数
func (db *CountingStateDB) GetState(addr common.Address, key common.Hash) common.Hash {
	stateKey := addr.Hex() + ":" + key.Hex()
	db.counter.RecordStateRead(stateKey)
	return db.StateDB.GetState(addr, key)
}

// SetState 重写SetState方法，增加计数
func (db *CountingStateDB) SetState(addr common.Address, key, value common.Hash) common.Hash {
	stateKey := addr.Hex() + ":" + key.Hex()
	db.counter.RecordStateRead(stateKey)
	db.counter.RecordStateWrite(stateKey)
	return db.StateDB.SetState(addr, key, value)
}

// GetBalance 重写GetBalance方法，增加计数
func (db *CountingStateDB) GetBalance(addr common.Address) *uint256.Int {
	db.counter.RecordAccountRead(addr.Hex())
	return db.StateDB.GetBalance(addr)
}

// SetBalance 重写SetBalance方法，增加计数
func (db *CountingStateDB) SetBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) {
	addrStr := addr.Hex()
	db.counter.RecordAccountRead(addrStr) // 写入前会先读取
	db.counter.RecordStateWrite(addrStr)
	db.StateDB.SetBalance(addr, amount, reason)
}

// GetNonce 重写GetNonce方法，增加计数
func (db *CountingStateDB) GetNonce(addr common.Address) uint64 {
	db.counter.RecordAccountRead(addr.Hex())
	return db.StateDB.GetNonce(addr)
}

// SetNonce 重写SetNonce方法，增加计数
func (db *CountingStateDB) SetNonce(addr common.Address, nonce uint64, reason tracing.NonceChangeReason) {
	addrStr := addr.Hex()
	db.counter.RecordAccountRead(addrStr) // 写入前会先读取
	db.counter.RecordStateWrite(addrStr)
	db.StateDB.SetNonce(addr, nonce, reason)
}

// SubBalance 重写SubBalance方法，增加计数
func (db *CountingStateDB) SubBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	addrStr := addr.Hex()
	db.counter.RecordAccountRead(addrStr) // 写入前会先读取
	db.counter.RecordStateWrite(addrStr)
	return db.StateDB.SubBalance(addr, amount, reason)
}

// AddBalance 重写AddBalance方法，增加计数
func (db *CountingStateDB) AddBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	addrStr := addr.Hex()
	db.counter.RecordAccountRead(addrStr) // 写入前会先读取
	db.counter.RecordStateWrite(addrStr)
	return db.StateDB.AddBalance(addr, amount, reason)
}

// 区块统计数据结构
type BlockStats struct {
	BlockNum           uint64        // 区块号
	TransactionCount   int           // 交易数
	ProcessTime        time.Duration // 交易处理时间
	RootGenTime        time.Duration // 根哈希生成时间
	CommitTime         time.Duration // 数据库提交时间
	TotalTime          time.Duration // 总时间
	ProcessTimePercent float64       // 处理时间占比
	RootGenTimePercent float64       // 根哈希时间占比
	CommitTimePercent  float64       // 提交时间占比
	ReadStates         int           // 读状态数量
	WriteStates        int           // 写状态数量
	CacheHitRate5      float64       // 最近5个区块缓存命中率
	CacheHitRate10     float64       // 最近10个区块缓存命中率
	CacheHitRate20     float64       // 最近20个区块缓存命中率
	CacheHitRate40     float64       // 最近40个区块缓存命中率
}

// 统计聚合结构
type StatsAggregator struct {
	Stats            []BlockStats  // 所有区块的统计数据
	OutputDir        string        // 输出目录
	BlockWindow      uint64        // 统计窗口大小(每隔多少区块打印一次)
	CsvWindow        uint64        // CSV输出窗口大小(每隔多少区块生成一个CSV)
	LastOutputBlock  uint64        // 上次输出统计的区块号
	LastCsvBlock     uint64        // 上次输出CSV的区块号
	TotalProcessed   int           // 总处理区块数
	TotalTransaction int           // 总交易数
	MaxProcessTime   time.Duration // 最大处理时间
	MaxRootGenTime   time.Duration // 最大根哈希生成时间
	MaxCommitTime    time.Duration // 最大提交时间
	MaxTotalTime     time.Duration // 最大总时间
}

// 创建新的统计聚合器
func NewStatsAggregator(outputDir string, blockWindow, csvWindow uint64) *StatsAggregator {
	// 确保输出目录存在
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		os.MkdirAll(outputDir, 0755)
	}

	return &StatsAggregator{
		Stats:           make([]BlockStats, 0),
		OutputDir:       outputDir,
		BlockWindow:     blockWindow,
		CsvWindow:       csvWindow,
		LastOutputBlock: 0,
		LastCsvBlock:    0,
		TotalProcessed:  0,
		MaxProcessTime:  0,
		MaxRootGenTime:  0,
		MaxCommitTime:   0,
		MaxTotalTime:    0,
	}
}

// 添加区块统计数据
func (s *StatsAggregator) AddBlockStats(stats BlockStats) {
	s.Stats = append(s.Stats, stats)
	s.TotalProcessed++
	s.TotalTransaction += stats.TransactionCount

	// 更新最大值
	if stats.ProcessTime > s.MaxProcessTime {
		s.MaxProcessTime = stats.ProcessTime
	}
	if stats.RootGenTime > s.MaxRootGenTime {
		s.MaxRootGenTime = stats.RootGenTime
	}
	if stats.CommitTime > s.MaxCommitTime {
		s.MaxCommitTime = stats.CommitTime
	}
	if stats.TotalTime > s.MaxTotalTime {
		s.MaxTotalTime = stats.TotalTime
	}

	// 检查是否需要打印统计信息
	if stats.BlockNum-s.LastOutputBlock >= s.BlockWindow {
		s.PrintStats()
		s.LastOutputBlock = stats.BlockNum
	}

	// 检查是否需要输出CSV
	if stats.BlockNum-s.LastCsvBlock >= s.CsvWindow {
		s.OutputCSV()
		s.LastCsvBlock = stats.BlockNum
		// 清空统计数据，释放内存
		s.Stats = make([]BlockStats, 0)
	}
}

// 计算平均值
func (s *StatsAggregator) CalculateAvg() (avgProcessTime, avgRootGenTime, avgCommitTime, avgTotalTime time.Duration,
	avgProcessPercent, avgRootGenPercent, avgCommitPercent, avgHitRate5, avgHitRate10, avgHitRate20, avgHitRate40 float64) {

	if len(s.Stats) == 0 {
		return
	}

	var totalProcessTime, totalRootGenTime, totalCommitTime, totalTotalTime time.Duration
	var totalProcessPercent, totalRootGenPercent, totalCommitPercent, totalHitRate5, totalHitRate10, totalHitRate20, totalHitRate40 float64

	for _, stat := range s.Stats {
		totalProcessTime += stat.ProcessTime
		totalRootGenTime += stat.RootGenTime
		totalCommitTime += stat.CommitTime
		totalTotalTime += stat.TotalTime
		totalProcessPercent += stat.ProcessTimePercent
		totalRootGenPercent += stat.RootGenTimePercent
		totalCommitPercent += stat.CommitTimePercent
		totalHitRate5 += stat.CacheHitRate5
		totalHitRate10 += stat.CacheHitRate10
		totalHitRate20 += stat.CacheHitRate20
		totalHitRate40 += stat.CacheHitRate40
	}

	count := float64(len(s.Stats))
	avgProcessTime = time.Duration(float64(totalProcessTime) / count)
	avgRootGenTime = time.Duration(float64(totalRootGenTime) / count)
	avgCommitTime = time.Duration(float64(totalCommitTime) / count)
	avgTotalTime = time.Duration(float64(totalTotalTime) / count)
	avgProcessPercent = totalProcessPercent / count
	avgRootGenPercent = totalRootGenPercent / count
	avgCommitPercent = totalCommitPercent / count
	avgHitRate5 = totalHitRate5 / count
	avgHitRate10 = totalHitRate10 / count
	avgHitRate20 = totalHitRate20 / count
	avgHitRate40 = totalHitRate40 / count

	return
}

// 打印统计信息
func (s *StatsAggregator) PrintStats() {
	if len(s.Stats) == 0 {
		return
	}

	avgProcessTime, avgRootGenTime, avgCommitTime, avgTotalTime,
		avgProcessPercent, avgRootGenPercent, avgCommitPercent,
		avgHitRate5, avgHitRate10, avgHitRate20, avgHitRate40 := s.CalculateAvg()

	fmt.Printf("===== 区块统计 (区块范围: %d - %d) =====\n",
		s.Stats[0].BlockNum, s.Stats[len(s.Stats)-1].BlockNum)
	fmt.Printf("处理区块数: %d, 总交易数: %d\n", s.TotalProcessed, s.TotalTransaction)
	fmt.Printf("平均时间 - 交易处理: %v (%.2f%%), 根哈希: %v (%.2f%%), 提交: %v (%.2f%%), 总计: %v\n",
		avgProcessTime, avgProcessPercent, avgRootGenTime, avgRootGenPercent,
		avgCommitTime, avgCommitPercent, avgTotalTime)
	fmt.Printf("最大时间 - 交易处理: %v, 根哈希: %v, 提交: %v, 总计: %v\n",
		s.MaxProcessTime, s.MaxRootGenTime, s.MaxCommitTime, s.MaxTotalTime)
	fmt.Printf("缓存命中率 - 5区块: %.2f%%, 10区块: %.2f%%, 20区块: %.2f%%, 40区块: %.2f%%\n",
		avgHitRate5*100, avgHitRate10*100, avgHitRate20*100, avgHitRate40*100)
	fmt.Println("=======================================")
}

// 输出CSV文件
func (s *StatsAggregator) OutputCSV() {
	if len(s.Stats) == 0 {
		return
	}

	filename := fmt.Sprintf("%s/block_stats_%d_to_%d.csv",
		s.OutputDir, s.Stats[0].BlockNum, s.Stats[len(s.Stats)-1].BlockNum)

	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("创建CSV文件失败: %v\n", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入CSV头
	headers := []string{
		"BlockNum", "TransactionCount", "ProcessTime(ms)", "RootGenTime(ms)", "CommitTime(ms)",
		"TotalTime(ms)", "ProcessPercent", "RootGenPercent", "CommitPercent",
		"ReadStates", "WriteStates", "HitRate5", "HitRate10", "HitRate20", "HitRate40",
	}
	writer.Write(headers)

	// 写入每个区块的统计数据
	for _, stat := range s.Stats {
		record := []string{
			strconv.FormatUint(stat.BlockNum, 10),
			strconv.Itoa(stat.TransactionCount),
			strconv.FormatInt(stat.ProcessTime.Milliseconds(), 10),
			strconv.FormatInt(stat.RootGenTime.Milliseconds(), 10),
			strconv.FormatInt(stat.CommitTime.Milliseconds(), 10),
			strconv.FormatInt(stat.TotalTime.Milliseconds(), 10),
			strconv.FormatFloat(stat.ProcessTimePercent, 'f', 2, 64),
			strconv.FormatFloat(stat.RootGenTimePercent, 'f', 2, 64),
			strconv.FormatFloat(stat.CommitTimePercent, 'f', 2, 64),
			strconv.Itoa(stat.ReadStates),
			strconv.Itoa(stat.WriteStates),
			strconv.FormatFloat(stat.CacheHitRate5, 'f', 4, 64),
			strconv.FormatFloat(stat.CacheHitRate10, 'f', 4, 64),
			strconv.FormatFloat(stat.CacheHitRate20, 'f', 4, 64),
			strconv.FormatFloat(stat.CacheHitRate40, 'f', 4, 64),
		}
		writer.Write(record)
	}

	fmt.Printf("已输出CSV文件: %s\n", filename)
}

// TestProcessCSVTransactions 测试处理CSV中的交易
func TestProcessTransactions(t *testing.T) {
	// 定义数据库路径
	dbDir := "E:\\ethdata\\geth_db"
	statsDir := "E:\\ethdata\\stats"

	// 创建统计聚合器，每100,000个区块打印一次统计，每1,000,000个区块生成一个CSV
	statsAgg := NewStatsAggregator(statsDir, 100000, 1000000)

	// 创建或打开持久化数据库
	ldb, err := leveldb.New(dbDir, 1024, 1024, "eth-process-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	db := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(db, nil)

	// 创建genesis区块和区块链
	//engine := ethash.NewFaker()
	gspec := &Genesis{
		Config: params.TestChainConfig,
		Alloc:  GenesisAlloc{},
	}
	genesis := gspec.MustCommit(db, trieDB)

	// 保存最后处理的区块和状态根
	lastProcessedBlock := genesis
	// 获取genesis区块的状态根
	var lastStateRoot common.Hash
	if header := genesis.Header(); header != nil {
		lastStateRoot = header.Root
	}

	// 创建状态访问计数器
	counter := NewStateAccessCounter(50) // 记录最近50个区块

	// 处理CSV文件的函数
	processCSVFile := func(filePath string, startBlock, endBlock uint64) {
		t.Logf("开始处理文件: %s，区块范围: %d - %d", filePath, startBlock, endBlock)

		// 打开CSV文件
		file, err := os.Open(filePath)
		if err != nil {
			t.Fatalf("无法打开CSV文件 %s: %v", filePath, err)
		}
		defer file.Close()

		// 解析CSV数据
		reader := csv.NewReader(file)
		headers, err := reader.Read()
		if err != nil {
			t.Fatalf("读取CSV头失败: %v", err)
		}
		t.Logf("CSV头: %v", headers)

		// 按区块组织交易
		msgsByBlock := make(map[uint64][]*Message)

		// 读取CSV数据并组织交易
		for {
			record, err := reader.Read()
			if err != nil {
				break
			}

			// 确保记录有足够的字段
			if len(record) < 9 { // 至少需要value字段
				t.Logf("跳过不完整的记录: %v", record)
				continue
			}

			blockNum := new(big.Int)
			blockNum.SetString(record[0], 10)

			// 检查区块是否在处理范围内
			if blockNum.Uint64() < startBlock || blockNum.Uint64() > endBlock {
				continue
			}

			// CSV格式: blockNumber timestamp transactionHash from to toCreate fromIsContract toIsContract value gasLimit gasPrice gasUsed callingFunction isError...
			from := common.HexToAddress(record[3])
			var to *common.Address
			if record[4] != "" {
				toAddr := common.HexToAddress(record[4])
				to = &toAddr
			}

			// 转换value
			value := new(big.Int)
			if record[8] != "" {
				value.SetString(record[8], 10)
			}

			// 设置gasLimit和gasPrice
			gasLimit := uint64(21000) // 默认值
			if len(record) > 9 && record[9] != "" {
				gl, err := strconv.ParseUint(record[9], 10, 64)
				if err == nil {
					gasLimit = gl
				}
			}

			gasPrice := big.NewInt(1000000000) // 默认值
			if len(record) > 10 && record[10] != "" {
				gp := new(big.Int)
				if _, ok := gp.SetString(record[10], 10); ok {
					gasPrice = gp
				}
			}

			// 处理callingFunction作为data
			var data []byte
			if len(record) > 12 && record[12] != "" && record[12] != "null" {
				data = common.FromHex(record[12])
			}

			// 创建消息
			msg := &Message{
				To:        to,
				From:      from,
				Nonce:     0, // 可以考虑从CSV中读取nonce
				Value:     value,
				GasLimit:  gasLimit,
				GasPrice:  gasPrice,
				GasFeeCap: gasPrice, // 对于旧交易，使用gasPrice作为GasFeeCap
				GasTipCap: gasPrice, // 对于旧交易，使用gasPrice作为GasTipCap
				Data:      data,
			}

			msgsByBlock[blockNum.Uint64()] = append(msgsByBlock[blockNum.Uint64()], msg)
		}

		// 计算区块范围
		var minBlock, maxBlock uint64 = endBlock, startBlock
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
		for blockNum := minBlock; blockNum <= maxBlock; blockNum++ {
			if len(msgsByBlock[blockNum]) == 0 {
				continue
			}

			// 计数器进入新区块
			counter.NextBlock(blockNum)

			// 创建新的区块
			header := &types.Header{
				ParentHash: parent.Hash(),
				Number:     new(big.Int).SetUint64(blockNum),
				GasLimit:   30000000,
				Time:       uint64(blockNum * 15),
				Difficulty: big.NewInt(1),
				BaseFee:    big.NewInt(1000000000),
			}

			// 创建statedb，使用上一个区块的状态根
			statedb, err := state.New(lastStateRoot, state.NewDatabase(trieDB, nil))
			if err != nil {
				t.Fatalf("创建状态失败: %v", err)
			}

			// 创建带计数功能的statedb
			countingStateDB := &CountingStateDB{
				StateDB: statedb,
				counter: counter,
			}

			bigBalance := new(big.Int).Mul(big.NewInt(1000000), big.NewInt(1e18))
			// 转换为uint256.Int
			balance, overflow := uint256.FromBig(bigBalance)
			if overflow {
				t.Fatalf("余额溢出")
			}

			// 为所有发送方预分配余额
			for _, msg := range msgsByBlock[blockNum] {
				countingStateDB.SetBalance(msg.From, balance, tracing.BalanceChangeUnspecified)
			}

			// 处理区块中的所有交易
			processStart := time.Now()
			gp := new(GasPool).AddGas(header.GasLimit)
			var usedGas uint64
			var receipts types.Receipts

			// 创建EVM上下文
			blockContext := vm.BlockContext{
				CanTransfer: CanTransfer,
				Transfer:    Transfer,
				GetHash:     func(n uint64) common.Hash { return common.Hash{} },
				Coinbase:    common.Address{},
				BlockNumber: new(big.Int).SetUint64(blockNum),
				Time:        header.Time,
				Difficulty:  header.Difficulty,
				GasLimit:    header.GasLimit,
				BaseFee:     header.BaseFee,
			}

			// 使用countingStateDB作为vm.StateDB
			vmenv := vm.NewEVM(blockContext, countingStateDB, params.TestChainConfig, vm.Config{})

			for _, msg := range msgsByBlock[blockNum] {
				// 处理交易
				result, err := ApplyMessage(vmenv, msg, gp)
				var receipt *types.Receipt
				if err != nil {
					// t.Logf("receipt err： %s", err.Error()) // 不再打印错误信息
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
			root := countingStateDB.IntermediateRoot(true)
			rootGenDuration := time.Since(rootGenStart)

			// 提交状态到数据库阶段
			commitStart := time.Now()
			err = trieDB.Commit(root, false)
			commitDuration := time.Since(commitStart)

			if err != nil {
				t.Fatalf("提交状态失败，区块 %d: %v", blockNum, err)
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
			rawdb.WriteHeadBlockHash(db, block.Hash())

			// 计算总时间和百分比
			totalTime := processDuration + rootGenDuration + commitDuration
			processPercent := float64(processDuration) / float64(totalTime) * 100
			rootGenPercent := float64(rootGenDuration) / float64(totalTime) * 100
			commitPercent := float64(commitDuration) / float64(totalTime) * 100

			// 获取缓存命中率
			hitRate5 := counter.GetCacheHitRate(5)
			hitRate10 := counter.GetCacheHitRate(10)
			hitRate20 := counter.GetCacheHitRate(20)
			hitRate40 := counter.GetCacheHitRate(40)

			// 创建区块统计数据
			blockStats := BlockStats{
				BlockNum:           blockNum,
				TransactionCount:   len(msgsByBlock[blockNum]),
				ProcessTime:        processDuration,
				RootGenTime:        rootGenDuration,
				CommitTime:         commitDuration,
				TotalTime:          totalTime,
				ProcessTimePercent: processPercent,
				RootGenTimePercent: rootGenPercent,
				CommitTimePercent:  commitPercent,
				ReadStates:         len(counter.ReadStates),
				WriteStates:        len(counter.WriteStates),
				CacheHitRate5:      hitRate5,
				CacheHitRate10:     hitRate10,
				CacheHitRate20:     hitRate20,
				CacheHitRate40:     hitRate40,
			}

			// 添加到统计聚合器
			statsAgg.AddBlockStats(blockStats)

			// 定期刷新数据库
			if blockNum%1000 == 0 {
				//flushStart := time.Now()
				trieDB.Cap(1024 * 1024 * 1024) // 1GB内存限制
				//flushDuration := time.Since(flushStart)
				//t.Logf("区块 %d - 数据库刷新时间: %v", blockNum, flushDuration)
			}
		}

		t.Logf("文件处理完成: %s", filePath)
	}

	// 查找所有匹配的CSV文件并按顺序排序
	dataDir := "E:\\ethdata\\0to999999_BlockTransaction"
	csvFiles, err := filepath.Glob(filepath.Join(dataDir, "*_BlockTransaction.csv"))
	if err != nil {
		t.Fatalf("查找CSV文件失败: %v", err)
	}

	// 创建一个切片存储文件和对应的区块范围
	type FileRange struct {
		path       string
		startBlock uint64
		endBlock   uint64
	}
	fileRanges := []FileRange{}

	// 解析所有文件的区块范围
	for _, csvFile := range csvFiles {
		baseName := filepath.Base(csvFile)
		parts := strings.Split(baseName, "_")
		if len(parts) != 2 {
			continue
		}

		// 解析文件名中的区块范围
		rangeParts := strings.Split(parts[0], "to")
		if len(rangeParts) != 2 {
			continue
		}

		startBlock := new(big.Int)
		startBlock.SetString(rangeParts[0], 10)

		endBlock := new(big.Int)
		endBlock.SetString(rangeParts[1], 10)

		fileRanges = append(fileRanges, FileRange{
			path:       csvFile,
			startBlock: startBlock.Uint64(),
			endBlock:   endBlock.Uint64(),
		})
	}

	// 按起始区块排序
	for i := 0; i < len(fileRanges); i++ {
		for j := i + 1; j < len(fileRanges); j++ {
			if fileRanges[i].startBlock > fileRanges[j].startBlock {
				fileRanges[i], fileRanges[j] = fileRanges[j], fileRanges[i]
			}
		}
	}

	// 按顺序处理所有文件
	for _, fr := range fileRanges {
		processCSVFile(fr.path, fr.startBlock, fr.endBlock)
	}

	// 处理完成后输出最终统计信息
	statsAgg.PrintStats()

	// 保存最后一批统计数据
	if len(statsAgg.Stats) > 0 {
		statsAgg.OutputCSV()
	}
}

// 确保CountingStateDB实现了vm.StateDB接口
var _ vm.StateDB = (*CountingStateDB)(nil)
