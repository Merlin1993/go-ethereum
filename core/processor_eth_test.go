package core

import (
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/triedb/hashdb"

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

/*
以太坊账户结构及状态访问说明:

以太坊的账户是作为一个完整的结构体存储的，包含以下字段:
1. Nonce: 交易计数，用于防止重放攻击
2. Balance: 账户余额
3. Root: 账户存储树的根哈希(对于合约账户)
4. CodeHash: 合约代码的哈希(对于合约账户)

任何修改账户的操作(如SetBalance、SetNonce等)都需要先读取整个账户，
修改其中的字段，然后再写回。这导致账户状态修改总是先读后写的模式。

而对于合约存储(由GetState/SetState操作)，每个存储槽位是单独读写的，
可以直接写入而无需先读取(除非业务逻辑本身需要先读取当前值再修改)。

因此在统计中，账户状态(地址级别)的操作通常表现为"读后写"，
而合约存储的操作则可能是"只读"或"只写"模式。
*/

// 自定义区块数量维度配置，用于缓存命中率统计
var CacheBlockSizes = []uint64{5, 10, 20, 40, 80, 160}

// StateAccessCounter 用于记录状态访问信息
type StateAccessCounter struct {
	AccountReads   map[string]bool            // 记录账户读取(只记录一次)
	BlockNum       uint64                     // 当前区块号
	RecentAccess   map[uint64]map[string]bool // 按区块号记录访问的状态
	MaxHistorySize uint64                     // 最大历史记录数量

	// 新增字段用于跟踪已经访问过的状态
	VisitedReads      map[string]bool // 本区块内已经读取过的状态
	VisitedWrites     map[string]bool // 本区块内已经写入过的状态
	UniqueReads       int             // 区块内唯一的读操作数量
	UniqueWrites      int             // 区块内唯一的写操作数量
	DetailedAccessLog bool            // 是否记录详细的访问日志
	ReadsOnly         map[string]bool // 只读状态
	ReadThenWritten   map[string]bool // 先读后写状态
	WritesOnly        map[string]bool // 只写状态

	// 新增字段，记录所有部署的合约地址
	ContractAddresses map[common.Address]bool // 记录所有部署的合约地址
}

// NewStateAccessCounter 创建一个新的状态访问计数器
func NewStateAccessCounter(maxHistorySize uint64) *StateAccessCounter {
	return &StateAccessCounter{
		AccountReads:      make(map[string]bool),
		RecentAccess:      make(map[uint64]map[string]bool),
		MaxHistorySize:    maxHistorySize,
		VisitedReads:      make(map[string]bool),
		VisitedWrites:     make(map[string]bool),
		DetailedAccessLog: true, // 开启详细日志
		ReadsOnly:         make(map[string]bool),
		ReadThenWritten:   make(map[string]bool),
		WritesOnly:        make(map[string]bool),
		ContractAddresses: make(map[common.Address]bool), // 初始化合约地址map
	}
}

// RecordStateRead 记录状态读取
func (c *StateAccessCounter) RecordStateRead(key string) {
	// 如果该键尚未被读取过且尚未被写入过，增加唯一读取计数
	if !c.VisitedReads[key] && !c.VisitedWrites[key] {
		c.UniqueReads++
		c.VisitedReads[key] = true

		// 记录详细访问日志
		if c.DetailedAccessLog {
			c.ReadsOnly[key] = true
		}
	}

	if c.RecentAccess[c.BlockNum] == nil {
		c.RecentAccess[c.BlockNum] = make(map[string]bool)
	}
	c.RecentAccess[c.BlockNum][key] = true
}

// RecordStateWrite 记录状态写入
func (c *StateAccessCounter) RecordStateWrite(key string) {
	// 如果该键尚未被写入过，增加唯一写入计数
	if !c.VisitedWrites[key] {
		c.UniqueWrites++
		c.VisitedWrites[key] = true

		// 记录详细访问日志
		if c.DetailedAccessLog {
			if c.ReadsOnly[key] {
				delete(c.ReadsOnly, key)
				c.ReadThenWritten[key] = true
			} else {
				c.WritesOnly[key] = true
			}
		}
	}

	if c.RecentAccess[c.BlockNum] == nil {
		c.RecentAccess[c.BlockNum] = make(map[string]bool)
	}
	c.RecentAccess[c.BlockNum][key] = true
}

// NextBlock 进入下一个区块，清除旧的状态记录
func (c *StateAccessCounter) NextBlock(blockNum uint64) {

	c.BlockNum = blockNum
	c.AccountReads = make(map[string]bool)
	c.VisitedReads = make(map[string]bool)
	c.VisitedWrites = make(map[string]bool)
	c.UniqueReads = 0
	c.UniqueWrites = 0
	c.ReadsOnly = make(map[string]bool)
	c.ReadThenWritten = make(map[string]bool)
	c.WritesOnly = make(map[string]bool)
	// 注意：不清除ContractAddresses，这是全局记录

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
	debug   bool // 是否启用调试模式
}

// GetState 重写GetState方法，增加计数
func (db *CountingStateDB) GetState(addr common.Address, key common.Hash) common.Hash {
	stateKey := addr.Hex() + ":" + key.Hex()
	if db.debug {
		fmt.Printf("读取状态: %s\n", stateKey)
	}
	// 合约存储的读取，是直接读取存储槽，不涉及到修改，因此是纯读取操作
	db.counter.RecordStateRead(stateKey)
	return db.StateDB.GetState(addr, key)
}

// SetState 重写SetState方法，增加计数
func (db *CountingStateDB) SetState(addr common.Address, key, value common.Hash) common.Hash {
	stateKey := addr.Hex() + ":" + key.Hex()
	if db.debug {
		fmt.Printf("写入状态: %s = %s\n", stateKey, value.Hex())
	}
	db.counter.RecordStateWrite(stateKey)
	return db.StateDB.SetState(addr, key, value)
}

// GetBalance 重写GetBalance方法，增加计数
func (db *CountingStateDB) GetBalance(addr common.Address) *uint256.Int {
	addrStr := addr.Hex()
	if db.debug {
		fmt.Printf("读取余额: %s\n", addrStr)
	}
	db.counter.RecordStateRead(addrStr)
	return db.StateDB.GetBalance(addr)
}

// SetBalance 重写SetBalance方法，增加计数
func (db *CountingStateDB) SetBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) {
	addrStr := addr.Hex()
	if db.debug {
		fmt.Printf("设置余额: %s = %s, 原因: %v\n", addrStr, amount.String(), reason)
	}
	// 以太坊账户是完整的结构体，设置余额操作需要先读取整个账户
	db.counter.RecordStateRead(addrStr) // 写入前会先读取，因为账户是一个结构体，这里只会更改结构体的一小部分，所以是先读后写
	db.counter.RecordStateWrite(addrStr)
	db.StateDB.SetBalance(addr, amount, reason)
}

// GetNonce 重写GetNonce方法，增加计数
func (db *CountingStateDB) GetNonce(addr common.Address) uint64 {
	addrStr := addr.Hex()
	if db.debug {
		fmt.Printf("读取Nonce: %s\n", addrStr)
	}
	db.counter.RecordStateRead(addrStr)
	return db.StateDB.GetNonce(addr)
}

// SetNonce 重写SetNonce方法，增加计数
func (db *CountingStateDB) SetNonce(addr common.Address, nonce uint64, reason tracing.NonceChangeReason) {
	addrStr := addr.Hex()
	if db.debug {
		fmt.Printf("设置Nonce: %s = %d, 原因: %v\n", addrStr, nonce, reason)
	}
	// 以太坊账户是完整的结构体，设置Nonce操作需要先读取整个账户
	db.counter.RecordStateRead(addrStr) // 写入前会先读取
	db.counter.RecordStateWrite(addrStr)
	db.StateDB.SetNonce(addr, nonce, reason)
}

// SubBalance 重写SubBalance方法，增加计数
func (db *CountingStateDB) SubBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	addrStr := addr.Hex()
	if db.debug {
		fmt.Printf("减少余额: %s - %s, 原因: %v\n", addrStr, amount.String(), reason)
	}
	// SubBalance 确实需要先读取当前余额
	db.counter.RecordStateRead(addrStr)
	db.counter.RecordStateWrite(addrStr)
	return db.StateDB.SubBalance(addr, amount, reason)
}

// AddBalance 重写AddBalance方法，增加计数
func (db *CountingStateDB) AddBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	addrStr := addr.Hex()
	if db.debug {
		fmt.Printf("增加余额: %s + %s, 原因: %v\n", addrStr, amount.String(), reason)
	}
	// AddBalance 确实需要先读取当前余额
	db.counter.RecordStateRead(addrStr)
	db.counter.RecordStateWrite(addrStr)
	return db.StateDB.AddBalance(addr, amount, reason)
}

// 区块统计数据结构
type BlockStats struct {
	BlockNum         uint64  // 区块号
	TransactionCount int     // 交易数
	SuccessCount     int     // 成功交易数
	SuccessRate      float64 // 交易成功率

	// 合约相关统计
	ContractTxCount      int     // 合约交易总数(创建+调用)
	ContractTxPercent    float64 // 合约交易占比
	ContractSuccessCount int     // 成功的合约交易数
	ContractSuccessRate  float64 // 合约交易成功率

	// 合约创建统计
	CreateContractCount   int     // 创建合约交易数
	CreateContractPercent float64 // 创建合约交易占比
	CreateSuccessCount    int     // 成功的创建合约数
	CreateSuccessRate     float64 // 创建合约成功率

	// 合约调用统计
	CallContractCount   int     // 调用合约交易数
	CallContractPercent float64 // 调用合约交易占比
	CallSuccessCount    int     // 成功的调用合约数
	CallSuccessRate     float64 // 调用合约成功率

	// 合约错误统计（新增）
	ErrorCount         int            // 错误总数
	ErrorReasons       map[string]int // 错误原因统计
	CreateErrorReasons map[string]int // 合约创建错误原因统计
	CallErrorReasons   map[string]int // 合约调用错误原因统计

	// 使用TrieBlockStats包含状态树相关统计
}

// 统计聚合结构
type StatsAggregator struct {
	Stats                []BlockStats         // 所有区块的统计数据
	TrieStatsAgg         *TrieStatsAggregator // 使用TrieStatsAggregator处理状态树统计
	OutputDir            string               // 输出目录
	BlockWindow          uint64               // 统计窗口大小(每隔多少区块打印一次)
	CsvWindow            uint64               // CSV输出窗口大小(每隔多少区块生成一个CSV)
	LastOutputBlock      uint64               // 上次输出统计的区块号
	LastCsvBlock         uint64               // 上次输出CSV的区块号
	TotalProcessed       int                  // 总处理区块数
	TotalTransaction     int                  // 总交易数
	TotalSuccess         int                  // 总成功交易数
	TotalContractTx      int                  // 总合约交易数
	TotalContractSuccess int                  // 总成功的合约交易数
}

// 创建新的统计聚合器
func NewStatsAggregator(outputDir string, blockWindow, csvWindow uint64) *StatsAggregator {
	// 确保输出目录存在
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		os.MkdirAll(outputDir, 0755)
	}

	return &StatsAggregator{
		Stats:                make([]BlockStats, 0),
		TrieStatsAgg:         NewTrieStatsAggregator(outputDir, "", StandardTrie), // 使用默认的StandardTrie类型
		OutputDir:            outputDir,
		BlockWindow:          blockWindow,
		CsvWindow:            csvWindow,
		LastOutputBlock:      0,
		LastCsvBlock:         0,
		TotalProcessed:       0,
		TotalTransaction:     0,
		TotalSuccess:         0,
		TotalContractTx:      0,
		TotalContractSuccess: 0,
	}
}

// 添加区块统计数据
func (s *StatsAggregator) AddBlockStats(stats BlockStats) {
	// 确保错误统计map不为nil
	if stats.ErrorReasons == nil {
		stats.ErrorReasons = make(map[string]int)
	}
	if stats.CreateErrorReasons == nil {
		stats.CreateErrorReasons = make(map[string]int)
	}
	if stats.CallErrorReasons == nil {
		stats.CallErrorReasons = make(map[string]int)
	}

	s.Stats = append(s.Stats, stats)
	s.TotalProcessed++
	s.TotalTransaction += stats.TransactionCount
	s.TotalSuccess += stats.SuccessCount
	s.TotalContractTx += stats.ContractTxCount
	s.TotalContractSuccess += stats.ContractSuccessCount

	// 同时记录到TrieStatsAggregator

	// 检查是否需要打印统计信息
	if stats.BlockNum-s.LastOutputBlock >= s.BlockWindow {
		// 统计信息由TrieStatsAggregator输出
		s.LastOutputBlock = stats.BlockNum
	}

	// 检查是否需要输出CSV
	if stats.BlockNum-s.LastCsvBlock >= s.CsvWindow {
		// CSV输出由TrieStatsAggregator处理
		s.LastCsvBlock = stats.BlockNum
		// 清空统计数据，释放内存
		s.Stats = make([]BlockStats, 0)
	}
}

// 打印统计信息
func (s *StatsAggregator) PrintStats() {
	// 使用TrieStatsAggregator输出统计信息
	s.TrieStatsAgg.OutputStats()
}

// 输出CSV文件
func (s *StatsAggregator) OutputCSV() {
	// 使用TrieStatsAggregator输出CSV
	s.TrieStatsAgg.OutputStats()
}

// 处理配置
type ProcessConfig struct {
	CommitInterval uint64 // 每处理多少个区块提交一次，默认1000
}

// 默认配置
func DefaultProcessConfig() ProcessConfig {
	return ProcessConfig{
		CommitInterval: 1000,
	}
}

// 自然数字排序函数，保证文件按照数字顺序排序（如：1, 2, ..., 10, 11，而不是1, 10, 11, 2, ...）
func naturalSort(files []string) {
	sort.Slice(files, func(i, j int) bool {
		// 提取文件名
		fileNameI := filepath.Base(files[i])
		fileNameJ := filepath.Base(files[j])

		// 从文件名中提取数字部分
		numStrI := ""
		numStrJ := ""

		// 提取transactions_或blocks_后面的数字部分
		if idx := strings.Index(fileNameI, "transactions_"); idx >= 0 {
			numStrI = fileNameI[idx+len("transactions_"):]
		} else if idx := strings.Index(fileNameI, "blocks_"); idx >= 0 {
			numStrI = fileNameI[idx+len("blocks_"):]
		}
		if idx := strings.Index(fileNameJ, "transactions_"); idx >= 0 {
			numStrJ = fileNameJ[idx+len("transactions_"):]
		} else if idx := strings.Index(fileNameJ, "blocks_"); idx >= 0 {
			numStrJ = fileNameJ[idx+len("blocks_"):]
		}

		// 去掉.csv后缀
		numStrI = strings.TrimSuffix(numStrI, ".csv")
		numStrJ = strings.TrimSuffix(numStrJ, ".csv")

		// 如果没有数字部分，按原始文件名排序
		if numStrI == "" || numStrJ == "" {
			return files[i] < files[j]
		}

		// 将数字部分转换为整数进行比较
		numI, errI := strconv.Atoi(numStrI)
		numJ, errJ := strconv.Atoi(numStrJ)

		// 如果无法转换为数字，按原始文件名排序
		if errI != nil || errJ != nil {
			return files[i] < files[j]
		}

		// 按数字大小排序
		return numI < numJ
	})
}

// 存储区块高度到时间戳的映射
var blockTimestamps = make(map[uint64]uint64)

// 从对应的区块文件中加载时间戳
func loadBlockTimestampsFromFile(dataDir string, fileIndex string) error {
	// 构建区块文件路径
	var blockFile string
	if fileIndex == "" {
		blockFile = filepath.Join(dataDir, "blocks.csv")
	} else {
		blockFile = filepath.Join(dataDir, fmt.Sprintf("blocks_%s.csv", fileIndex))
	}

	// 检查文件是否存在
	if _, err := os.Stat(blockFile); os.IsNotExist(err) {
		return fmt.Errorf("区块文件不存在: %s", blockFile)
	}

	// 打开CSV文件
	csvFile, err := os.Open(blockFile)
	if err != nil {
		return fmt.Errorf("无法打开区块CSV文件 %s: %v", blockFile, err)
	}
	defer csvFile.Close()

	// 解析CSV数据
	reader := csv.NewReader(csvFile)
	// 读取标题行
	headers, err := reader.Read()
	if err != nil {
		return fmt.Errorf("读取区块CSV头失败: %v", err)
	}

	// 查找number和timestamp字段的索引
	var numberIdx, timestampIdx int = -1, -1
	for i, header := range headers {
		if header == "number" {
			numberIdx = i
		} else if header == "timestamp" {
			timestampIdx = i
		}
	}

	if numberIdx == -1 || timestampIdx == -1 {
		return fmt.Errorf("区块CSV文件 %s 缺少必要的字段", blockFile)
	}

	// 读取CSV数据并提取区块高度和时间戳
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		// 解析区块高度
		num, err := strconv.ParseUint(record[numberIdx], 10, 64)
		if err != nil {
			continue
		}

		// 解析时间戳
		timestamp, err := strconv.ParseUint(record[timestampIdx], 10, 64)
		if err != nil {
			continue
		}

		// 存储区块高度和时间戳的映射
		blockTimestamps[num] = timestamp
	}

	return nil
}

// 获取交易文件的索引部分
func getFileIndex(filePath string) string {
	fileName := filepath.Base(filePath)

	// 如果是没有索引的文件（如transactions.csv, blocks.csv）
	if !strings.Contains(fileName, "_") {
		return ""
	}

	// 提取索引部分
	parts := strings.Split(fileName, "_")
	if len(parts) < 2 {
		return ""
	}

	// 移除.csv后缀
	return strings.TrimSuffix(parts[1], ".csv")
}

// 获取指定区块的时间戳
func getBlockTimestamp(dataDir string, blockNum uint64) (uint64, error) {
	// 如果已经缓存了该区块的时间戳，直接返回
	if timestamp, ok := blockTimestamps[blockNum]; ok {
		return timestamp, nil
	}

	// 查找所有blocks*.csv文件
	pattern := filepath.Join(dataDir, "blocks*.csv")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return 0, fmt.Errorf("查找区块CSV文件失败: %v", err)
	}

	if len(files) == 0 {
		return 0, fmt.Errorf("未找到任何区块文件")
	}

	// 按自然数排序文件
	naturalSort(files)

	// 逐个文件查找区块
	for _, file := range files {
		// 打开CSV文件
		csvFile, err := os.Open(file)
		if err != nil {
			continue // 跳过无法打开的文件
		}
		defer csvFile.Close()

		// 解析CSV数据
		reader := csv.NewReader(csvFile)
		// 读取标题行
		headers, err := reader.Read()
		if err != nil {
			continue // 跳过无法读取标题的文件
		}

		// 查找number和timestamp字段的索引
		var numberIdx, timestampIdx int = -1, -1
		for i, header := range headers {
			if header == "number" {
				numberIdx = i
			} else if header == "timestamp" {
				timestampIdx = i
			}
		}

		if numberIdx == -1 || timestampIdx == -1 {
			continue // 跳过缺少必要字段的文件
		}

		// 读取CSV数据并查找目标区块
		for {
			record, err := reader.Read()
			if err != nil {
				break
			}

			// 解析区块高度
			num, err := strconv.ParseUint(record[numberIdx], 10, 64)
			if err != nil {
				continue
			}

			// 找到目标区块
			if num == blockNum {
				// 解析时间戳
				timestamp, err := strconv.ParseUint(record[timestampIdx], 10, 64)
				if err != nil {
					return 0, err
				}
				// 缓存时间戳
				blockTimestamps[blockNum] = timestamp
				return timestamp, nil
			}
		}
	}

	// 没有找到区块，返回默认时间戳
	return blockNum * 15, nil
}

// 查找所有匹配的CSV文件并按顺序排序
func findTransactionFiles(dataDir string) ([]string, error) {
	// 使用通配符匹配所有transactions_*.csv文件
	pattern := filepath.Join(dataDir, "transactions_*.csv")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("查找CSV文件失败: %v", err)
	}

	// 按自然数排序
	naturalSort(files)
	return files, nil
}

// TestProcessTransactions 测试处理CSV中的交易
func TestProcessTransactions(t *testing.T) {
	// 定义数据库路径
	dbDir := "F:\\ethdata\\geth_db"
	statsDir := "F:\\ethdata\\stats"
	dataDir := "E:\\ethdata"

	var maxBlockNum uint64 = 5000000
	var startNum uint64 = 46147

	// 创建统计聚合器，每100,000个区块打印一次统计，每1,000,000个区块生成一个CSV
	statsAgg := NewStatsAggregator(statsDir, 100000, 1000000)

	// 设置TrieStatsAggregator的数据路径
	statsAgg.TrieStatsAgg.DataPath = dbDir

	// 创建或打开持久化数据库
	ldb, err := leveldb.New(dbDir, 1024, 1024, "eth-process-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	db := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(db, &triedb.Config{
		Preimages: false,
		IsVerkle:  false,
		CacheTrie: common.UseCacheTrie,
		ReadCache: false,
		StartNum:  startNum,
		HashDB:    hashdb.Defaults,
	})
	//snaps, _ := snapshot.New(snapshot.Config{CacheSize: 100}, db, trieDB, types.EmptyRootHash)
	sdb := state.NewDatabase(trieDB, nil)

	// 创建genesis区块和区块链
	gspec := &Genesis{
		Config: params.TestChainConfig,
		Alloc:  GenesisAlloc{},
	}
	genesis := gspec.MustCommit(db, trieDB)

	// 保存最后处理的区块和状态根
	lastProcessedBlock := genesis
	// 获取genesis区块的状态根
	var lastStateRoot common.Hash
	if genesis != nil {
		if header := genesis.Header(); header != nil {
			lastStateRoot = header.Root
		}
	}
	t.Logf("state:%s", lastStateRoot.String())

	// 创建状态访问计数器
	counter := NewStateAccessCounter(160) // 记录最近160个区块的访问历史

	// 使用新的数据文件路径
	files, err := findTransactionFiles(dataDir)
	if err != nil {
		t.Fatalf("查找CSV文件失败: %v", err)
	}

	if len(files) == 0 {
		t.Fatalf("未找到任何交易文件")
	}

	t.Logf("找到 %d 个交易文件", len(files))
	for i, file := range files {
		t.Logf("文件 %d: %s", i+1, file)
	}

	// 依次处理每个文件
	for i, file := range files {
		t.Logf("开始处理第 %d/%d 个文件: %s", i+1, len(files), file)

		// 获取文件索引，用于加载对应的区块文件
		fileIndex := getFileIndex(file)

		// 加载对应的区块时间戳
		err := loadBlockTimestampsFromFile(dataDir, fileIndex)
		if err != nil {
			t.Logf("加载区块时间戳失败: %v", err)
			t.Logf("将使用默认时间戳计算方式")
		} else {
			t.Logf("成功加载区块时间戳，当前缓存区块数: %d", len(blockTimestamps))
		}

		// 打开CSV文件
		csvFile, err := os.Open(file)
		if err != nil {
			t.Fatalf("无法打开CSV文件 %s: %v", file, err)
		}
		defer csvFile.Close()

		// 解析CSV数据
		reader := csv.NewReader(csvFile)
		reader.Comma = ',' // 设置分隔符为逗号
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

			// 检查区块是否在处理范围内
			if blockNum < 0 || blockNum > maxBlockNum {
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
			msg := &Message{
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
			if ct != nil {
				result := ct.GetCleanupResult()
				if result != (common.Hash{}) {
					lastStateRoot = result
				}
			}

			if len(msgsByBlock[blockNum]) == 0 {
				continue
			}

			// 计数器进入新区块
			counter.NextBlock(blockNum)

			// 获取区块时间戳，如果没有则使用默认计算方式
			blockTime := uint64(blockNum * 15)
			if timestamp, ok := blockTimestamps[blockNum]; ok {
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
			countingStateDB := &CountingStateDB{
				StateDB: statedb,
				counter: counter,
				debug:   false,
			}

			bigBalance := new(big.Int).Mul(big.NewInt(1e15), big.NewInt(1e18))
			// 转换为uint256.Int
			balance, overflow := uint256.FromBig(bigBalance)
			if overflow {
				t.Fatalf("余额溢出")
			}

			// 为所有发送方预分配余额
			for _, msg := range msgsByBlock[blockNum] {
				if common.DebugFlag && msg.From == common.HexToAddress("0x4962f6533141e9e12B9e1846AB549c7042A40098") && msg.Nonce >= 6 {
					//currentNonce := countingStateDB.GetNonce(msg.From)
					//t.Logf("cn : %v, msgn: %v", currentNonce, msg.Nonce)

					//s.trie.GetAccount(common.HexToAddress("0xFD2605a2bF58fDbB90db1Da55dF61628B47F9e8c"))
				}
				countingStateDB.SetBalance(msg.From, balance, tracing.BalanceChangeUnspecified)
			}

			// 处理区块中的所有交易
			processStart := time.Now()
			gp := new(GasPool).AddGas(header.GasLimit)
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

			// 错误统计（新增）
			var errorCount int
			errorReasons := make(map[string]int)
			createErrorReasons := make(map[string]int)
			callErrorReasons := make(map[string]int)

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
			vmenv := vm.NewEVM(blockContext, countingStateDB, params.MainnetChainConfig, vm.Config{})

			for _, msg := range msgsByBlock[blockNum] {

				// 处理交易
				result, err := ApplyMessage(vmenv, msg, gp)
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
						//if result.ContractAddress == common.HexToAddress("0xa50156cF80fa9eC2e16899E4fb7e072300787417") {
						//	result.ContractAddress = common.HexToAddress("0xa50156cF80fa9eC2e16899E4fb7e072300787417")
						//}
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

				if err != nil || result.Err != nil {
					// 记录错误
					errorCount++
					var errReason string
					if err != nil {
						errReason = err.Error()
					} else if result.Err != nil {
						errReason = result.Err.Error()
					} else {
						errReason = "未知错误"
					}

					// 简化错误原因，提取主要错误类型
					errReason = simplifyErrorReason(errReason)

					// 更新错误计数
					if !(!isContractTx && (errReason == "fail get code" || errReason == "invalid jumpi destination" || errReason == "fail get account code")) {
						errorReasons[errReason]++
					}

					if common.DebugFlag && isContractTx && errReason == "fail get code" {
						// 打印失败的合约地址及其codeHash情况
						var contractAddr common.Address
						if msg.To != nil {
							contractAddr = *msg.To

							codeHash := countingStateDB.GetCodeHash(contractAddr)
							code := countingStateDB.GetCode(contractAddr)

							fmt.Printf("区块 %d: 合约调用失败 'fail get code'，地址: %s, codeHash: %s, codeSize: %d\n",
								blockNum, contractAddr.Hex(), codeHash.Hex(), len(code))

							// 查看该地址是否有余额和nonce
							balance := countingStateDB.GetBalance(contractAddr)
							nonce := countingStateDB.GetNonce(contractAddr)
							fmt.Printf("  余额: %s, Nonce: %d\n", balance.String(), nonce)

							// 检查该地址是否在我们的合约地址记录中
							if counter.ContractAddresses[contractAddr] {
								fmt.Printf("  该地址在合约地址记录中存在\n")
							} else {
								fmt.Printf("  该地址在合约地址记录中不存在\n")
							}

							// 检查账户是否存在于状态数据库中
							exists := countingStateDB.Exist(contractAddr)
							fmt.Printf("  账户在状态数据库中%s\n", map[bool]string{true: "存在", false: "不存在"}[exists])

							if len(code) > 0 {
								fmt.Printf("  账户有代码，长度: %d bytes\n", len(code))
							} else {
								fmt.Printf("  账户没有代码\n")
							}

							// 检查存储根
							storageRoot := countingStateDB.GetStorageRoot(contractAddr)
							fmt.Printf("  存储根: %s\n", storageRoot.Hex())

							// 打印一些交易信息
							fmt.Printf("  交易数据长度: %d bytes\n", len(msg.Data))
							if len(msg.Data) >= 4 {
								fmt.Printf("  交易函数选择器: 0x%x\n", msg.Data[:4])
							}
						} else {
							// 合约创建失败
							fmt.Printf("区块 %d: 合约创建失败 'fail get code'\n", blockNum)
						}
					}

					// 根据合约类型更新特定错误计数
					if isContractCreate {
						createErrorReasons[errReason]++
					} else if isContractTx {
						callErrorReasons[errReason]++
					}

					// t.Logf("receipt err： %s", errReason) // 不再打印每个错误信息
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
			root := lastStateRoot
			if common.UseCacheTrie {
				countingStateDB.PreCommit(false)
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
				sBlockNum := blockNum
				go func() {
					sRoot, _ := countingStateDB.PostCommit(sBlockNum, false, false)
					// 提交状态到数据库阶段 - 只在达到配置的间隔时才提交
					err = trieDB.Commit(sRoot, false)

					// 完成清理操作，设置结果哈希
					trieDB.CacheTrie().FinishCleanup(sBlockNum, sRoot)
					if err != nil {
						t.Fatalf("提交状态失败，区块 %d: %v", sBlockNum, err)
					}

					// 刷新数据库，避免内存占用过大
					trieDB.Cap(1024 * 1024 * 1024) // 1GB内存限制
				}()
			} else {
				root, _ = countingStateDB.Commit(blockNum, false, false)
				// 提交状态到数据库阶段 - 只在达到配置的间隔时才提交
				if blockNum-lastCommitBlock >= 1000 { // 每1000个区块提交一次
					err = trieDB.Commit(root, false)
					if err != nil {
						t.Fatalf("提交状态失败，区块 %d: %v", blockNum, err)
					}
					lastCommitBlock = blockNum

					// 刷新数据库，避免内存占用过大
					trieDB.Cap(1024 * 1024 * 1024) // 1GB内存限制
				}
			}
			rootGenDuration := time.Since(rootGenStart)

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

			// 直接记录到trie统计器中
			RecordTrieStats(statsAgg.TrieStatsAgg, blockNum, counter.UniqueWrites, counter.UniqueReads,
				len(msgsByBlock[blockNum]), processDuration, rootGenDuration)
		}
		t.Logf("完成处理文件: %s", file)
	}

	// 处理完成后输出最终统计信息
	statsAgg.PrintStats()

	// 保存最后一批统计数据
	if len(statsAgg.Stats) > 0 {
		// 使用StatsAggregator输出CSV文件
		statsAgg.OutputCSV()
	}
}

// 确保CountingStateDB实现了vm.StateDB接口
var _ vm.StateDB = (*CountingStateDB)(nil)

// OutputAccessStats 输出状态访问统计
func (c *StateAccessCounter) OutputAccessStats() {
	if !c.DetailedAccessLog {
		return
	}

	// 统计账户访问 vs 存储访问
	accountReads := 0
	accountWrites := 0
	storageReads := 0
	storageWrites := 0

	// 统计只读账户、读后写账户
	accountReadOnly := 0
	accountReadWrite := 0
	storageReadOnly := 0
	storageReadWrite := 0
	storageWriteOnly := 0

	for key := range c.ReadsOnly {
		if !strings.Contains(key, ":") {
			accountReadOnly++
		} else {
			storageReadOnly++
		}
	}

	for key := range c.ReadThenWritten {
		if !strings.Contains(key, ":") {
			accountReadWrite++
			accountReads++
			accountWrites++
		} else {
			storageReadWrite++
			storageReads++
			storageWrites++
		}
	}

	for key := range c.WritesOnly {
		if !strings.Contains(key, ":") {
			accountWrites++
		} else {
			storageWriteOnly++
			storageWrites++
		}
	}

	fmt.Printf("\n===== 状态访问详细统计 (区块 %d) =====\n", c.BlockNum)
	fmt.Printf("总唯一状态数: %d (读: %d, 写: %d)\n",
		len(c.ReadsOnly)+len(c.ReadThenWritten)+len(c.WritesOnly),
		c.UniqueReads, c.UniqueWrites)

	// 账户访问统计
	fmt.Printf("\n## 账户访问统计 ##\n")
	fmt.Printf("账户总读取: %d, 账户总写入: %d\n", accountReads+accountReadOnly+accountReadWrite, accountWrites+accountReadWrite)
	fmt.Printf("只读账户: %d, 读后写账户: %d\n", accountReadOnly, accountReadWrite)

	// 存储访问统计
	fmt.Printf("\n## 存储访问统计 ##\n")
	fmt.Printf("存储总读取: %d, 存储总写入: %d\n", storageReads+storageReadOnly+storageReadWrite, storageWrites+storageWriteOnly+storageReadWrite)
	fmt.Printf("只读存储: %d, 读后写存储: %d, 只写存储: %d\n", storageReadOnly, storageReadWrite, storageWriteOnly)
}

// simplifyErrorReason 简化错误原因，归类常见错误
func simplifyErrorReason(errMsg string) string {
	// 常见错误类型
	if strings.Contains(errMsg, "out of gas") || strings.Contains(errMsg, "gas required exceeds allowance") {
		return "燃料不足"
	} else if strings.Contains(errMsg, "execution reverted") {
		if strings.Contains(errMsg, "execution reverted: ") {
			// 提取revert原因，如果有
			parts := strings.SplitN(errMsg, "execution reverted: ", 2)
			if len(parts) > 1 && len(parts[1]) > 0 {
				return "执行回退: " + parts[1]
			}
		}
		return "执行回退"
	} else if strings.Contains(errMsg, "invalid opcode") {
		return "无效操作码"
	} else if strings.Contains(errMsg, "stack overflow") || strings.Contains(errMsg, "stack underflow") {
		return "栈溢出/下溢"
	} else if strings.Contains(errMsg, "nonce too high") || strings.Contains(errMsg, "nonce too low") {
		return "Nonce错误"
	} else if strings.Contains(errMsg, "insufficient balance") {
		return "余额不足"
	} else if strings.Contains(errMsg, "invalid jump destination") {
		return "无效跳转目标"
	} else if strings.Contains(errMsg, "code size") || strings.Contains(errMsg, "code length") {
		return "代码大小错误"
	} else if strings.Contains(errMsg, "max code size exceeded") {
		return "代码大小超限"
	} else if strings.Contains(errMsg, "max initcode size exceeded") {
		return "初始化代码大小超限"
	} else if strings.Contains(errMsg, "intrinsic gas too low") {
		return "内在燃料不足"
	} else if strings.Contains(errMsg, "sender doesn't have enough funds") {
		return "发送方资金不足"
	} else if strings.Contains(errMsg, "reached the EIP-170 contract code size limit") {
		return "合约代码大小达到EIP-170限制"
	} else if strings.Contains(errMsg, "reached the EIP-3860 initcode size limit") {
		return "初始化代码大小达到EIP-3860限制"
	} else if strings.Contains(errMsg, "failed to execute call") {
		return "执行调用失败"
	}

	// 其他错误，保留前50个字符并添加省略号
	if len(errMsg) > 50 {
		return errMsg[:50] + "..."
	}
	return errMsg
}
