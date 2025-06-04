package core

import (
	"encoding/csv"
	"fmt"
	"github.com/ethereum/go-ethereum/rlp"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/cacheTrie"
	"github.com/ethereum/go-ethereum/core/state/snapshot"

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

	ProcessTime        time.Duration // 交易处理时间
	RootGenTime        time.Duration // 根哈希生成时间
	CommitTime         time.Duration // 数据库提交时间
	TotalTime          time.Duration // 总时间
	ProcessTimePercent float64       // 处理时间占比
	RootGenTimePercent float64       // 根哈希时间占比

	UniqueReads  int // 唯一读状态数
	UniqueWrites int // 唯一写状态数

	AvgStatesPerTx         float64 // 每个交易平均状态访问数
	AvgStatesPerContractTx float64 // 每个合约交易平均状态访问数
	AvgStatesPerCreateTx   float64 // 每个创建合约交易平均状态访问数
	AvgStatesPerCallTx     float64 // 每个调用合约交易平均状态访问数

	CacheHitRate5   float64 // 最近5个区块缓存命中率
	CacheHitRate10  float64 // 最近10个区块缓存命中率
	CacheHitRate20  float64 // 最近20个区块缓存命中率
	CacheHitRate40  float64 // 最近40个区块缓存命中率
	CacheHitRate80  float64 // 最近80个区块缓存命中率
	CacheHitRate160 float64 // 最近160个区块缓存命中率
}

// 统计聚合结构
type StatsAggregator struct {
	Stats                []BlockStats // 所有区块的统计数据
	OutputDir            string       // 输出目录
	BlockWindow          uint64       // 统计窗口大小(每隔多少区块打印一次)
	CsvWindow            uint64       // CSV输出窗口大小(每隔多少区块生成一个CSV)
	LastOutputBlock      uint64       // 上次输出统计的区块号
	LastCsvBlock         uint64       // 上次输出CSV的区块号
	TotalProcessed       int          // 总处理区块数
	TotalTransaction     int          // 总交易数
	TotalSuccess         int          // 总成功交易数
	TotalContractTx      int          // 总合约交易数
	TotalContractSuccess int          // 总成功的合约交易数

	MaxProcessTime  time.Duration // 最大处理时间
	MaxRootGenTime  time.Duration // 最大根哈希生成时间
	MaxCommitTime   time.Duration // 最大提交时间
	MaxTotalTime    time.Duration // 最大总时间
	MaxTxCount      int           // 最大交易数
	MaxReadStates   int           // 最大读状态数
	MaxWriteStates  int           // 最大写状态数
	MaxUniqueReads  int           // 最大唯一读状态数
	MaxUniqueWrites int           // 最大唯一写状态数

	MinHitRate5   float64 // 最小5区块命中率
	MinHitRate10  float64 // 最小10区块命中率
	MinHitRate20  float64 // 最小20区块命中率
	MinHitRate40  float64 // 最小40区块命中率
	MinHitRate80  float64 // 最小80区块命中率
	MinHitRate160 float64 // 最小160区块命中率
}

// 创建新的统计聚合器
func NewStatsAggregator(outputDir string, blockWindow, csvWindow uint64) *StatsAggregator {
	// 确保输出目录存在
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		os.MkdirAll(outputDir, 0755)
	}

	return &StatsAggregator{
		Stats:                make([]BlockStats, 0),
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
		MaxUniqueReads:       0,
		MaxUniqueWrites:      0,
		MinHitRate5:          1.0, // 初始化为最大值1.0
		MinHitRate10:         1.0,
		MinHitRate20:         1.0,
		MinHitRate40:         1.0,
		MinHitRate80:         1.0, // 新增80区块命中率
		MinHitRate160:        1.0, // 新增160区块命中率
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
	if stats.TransactionCount > s.MaxTxCount {
		s.MaxTxCount = stats.TransactionCount
	}
	if stats.UniqueReads > s.MaxUniqueReads {
		s.MaxUniqueReads = stats.UniqueReads
	}
	if stats.UniqueWrites > s.MaxUniqueWrites {
		s.MaxUniqueWrites = stats.UniqueWrites
	}

	// 更新最小命中率
	if stats.CacheHitRate5 < s.MinHitRate5 {
		s.MinHitRate5 = stats.CacheHitRate5
	}
	if stats.CacheHitRate10 < s.MinHitRate10 {
		s.MinHitRate10 = stats.CacheHitRate10
	}
	if stats.CacheHitRate20 < s.MinHitRate20 {
		s.MinHitRate20 = stats.CacheHitRate20
	}
	if stats.CacheHitRate40 < s.MinHitRate40 {
		s.MinHitRate40 = stats.CacheHitRate40
	}
	if stats.CacheHitRate80 < s.MinHitRate80 {
		s.MinHitRate80 = stats.CacheHitRate80
	}
	if stats.CacheHitRate160 < s.MinHitRate160 {
		s.MinHitRate160 = stats.CacheHitRate160
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
	avgProcessPercent, avgRootGenPercent, avgCommitPercent, avgHitRate5, avgHitRate10, avgHitRate20, avgHitRate40, avgHitRate80, avgHitRate160 float64,
	avgTxCount, avgReadStates, avgWriteStates float64) {

	if len(s.Stats) == 0 {
		return
	}

	var totalProcessTime, totalRootGenTime, totalCommitTime, totalTotalTime time.Duration
	var totalProcessPercent, totalRootGenPercent, totalHitRate5, totalHitRate10, totalHitRate20, totalHitRate40, totalHitRate80, totalHitRate160 float64
	var totalTxCount, totalReadStates, totalWriteStates int

	for _, stat := range s.Stats {
		totalProcessTime += stat.ProcessTime
		totalRootGenTime += stat.RootGenTime
		totalCommitTime += stat.CommitTime
		totalTotalTime += stat.TotalTime
		totalProcessPercent += stat.ProcessTimePercent
		totalRootGenPercent += stat.RootGenTimePercent
		totalHitRate5 += stat.CacheHitRate5
		totalHitRate10 += stat.CacheHitRate10
		totalHitRate20 += stat.CacheHitRate20
		totalHitRate40 += stat.CacheHitRate40
		totalHitRate80 += stat.CacheHitRate80
		totalHitRate160 += stat.CacheHitRate160
		totalTxCount += stat.TransactionCount
	}

	count := float64(len(s.Stats))
	avgProcessTime = time.Duration(float64(totalProcessTime) / count)
	avgRootGenTime = time.Duration(float64(totalRootGenTime) / count)
	avgTotalTime = time.Duration(float64(totalTotalTime) / count)
	avgProcessPercent = totalProcessPercent / count
	avgRootGenPercent = totalRootGenPercent / count
	avgHitRate5 = totalHitRate5 / count
	avgHitRate10 = totalHitRate10 / count
	avgHitRate20 = totalHitRate20 / count
	avgHitRate40 = totalHitRate40 / count
	avgHitRate80 = totalHitRate80 / count
	avgHitRate160 = totalHitRate160 / count
	avgTxCount = float64(totalTxCount) / count
	avgReadStates = float64(totalReadStates) / count
	avgWriteStates = float64(totalWriteStates) / count

	return
}

// 打印统计信息
func (s *StatsAggregator) PrintStats() {
	if len(s.Stats) == 0 {
		return
	}

	// 只取最近的BlockWindow个区块或者全部（如果数量不足）
	startIdx := 0
	if len(s.Stats) > int(s.BlockWindow) {
		startIdx = len(s.Stats) - int(s.BlockWindow)
	}
	recentStats := s.Stats[startIdx:]

	// 计算这部分的统计数据
	var totalProcessTime, totalRootGenTime, totalCommitTime, totalTotalTime time.Duration
	var totalProcessPercent, totalRootGenPercent float64
	var totalTxCount, totalSuccessCount, totalContractTxCount, totalContractSuccessCount int
	var totalUniqueReads, totalUniqueWrites int

	var maxProcessTime, maxRootGenTime, maxCommitTime, maxTotalTime time.Duration
	var maxTxCount, maxUniqueReads, maxUniqueWrites int

	// 收集所有区块的状态访问情况，用于计算整体命中率
	accessedStates := make(map[string]bool)    // 所有访问过的状态
	cacheHitStates5 := make(map[string]bool)   // 命中5区块缓存的状态
	cacheHitStates10 := make(map[string]bool)  // 命中10区块缓存的状态
	cacheHitStates20 := make(map[string]bool)  // 命中20区块缓存的状态
	cacheHitStates40 := make(map[string]bool)  // 命中40区块缓存的状态
	cacheHitStates80 := make(map[string]bool)  // 命中80区块缓存的状态
	cacheHitStates160 := make(map[string]bool) // 命中160区块缓存的状态

	// 用于跟踪每个区块的状态访问
	blockStateAccess := make(map[uint64]map[string]bool)
	blockStateHits5 := make(map[uint64]map[string]bool)
	blockStateHits10 := make(map[uint64]map[string]bool)
	blockStateHits20 := make(map[uint64]map[string]bool)
	blockStateHits40 := make(map[uint64]map[string]bool)
	blockStateHits80 := make(map[uint64]map[string]bool)
	blockStateHits160 := make(map[uint64]map[string]bool)

	// 交易统计
	var totalSuccessRate, totalContractTxPercent, totalContractSuccessRate float64
	var totalAvgStatesPerTx, totalAvgStatesPerContractTx float64

	for _, stat := range recentStats {
		totalProcessTime += stat.ProcessTime
		totalRootGenTime += stat.RootGenTime
		totalCommitTime += stat.CommitTime
		totalTotalTime += stat.TotalTime
		totalProcessPercent += stat.ProcessTimePercent
		totalRootGenPercent += stat.RootGenTimePercent
		totalTxCount += stat.TransactionCount
		totalSuccessCount += stat.SuccessCount
		totalContractTxCount += stat.ContractTxCount
		totalContractSuccessCount += stat.ContractSuccessCount
		totalUniqueReads += stat.UniqueReads
		totalUniqueWrites += stat.UniqueWrites
		totalSuccessRate += stat.SuccessRate
		totalContractTxPercent += stat.ContractTxPercent
		totalContractSuccessRate += stat.ContractSuccessRate
		totalAvgStatesPerTx += stat.AvgStatesPerTx
		totalAvgStatesPerContractTx += stat.AvgStatesPerContractTx

		// 计算最大值
		if stat.ProcessTime > maxProcessTime {
			maxProcessTime = stat.ProcessTime
		}
		if stat.RootGenTime > maxRootGenTime {
			maxRootGenTime = stat.RootGenTime
		}
		if stat.CommitTime > maxCommitTime {
			maxCommitTime = stat.CommitTime
		}
		if stat.TotalTime > maxTotalTime {
			maxTotalTime = stat.TotalTime
		}
		if stat.TransactionCount > maxTxCount {
			maxTxCount = stat.TransactionCount
		}
		if stat.UniqueReads > maxUniqueReads {
			maxUniqueReads = stat.UniqueReads
		}
		if stat.UniqueWrites > maxUniqueWrites {
			maxUniqueWrites = stat.UniqueWrites
		}

		// 为每个区块创建状态访问跟踪
		blockNum := stat.BlockNum
		blockStateAccess[blockNum] = make(map[string]bool)
		blockStateHits5[blockNum] = make(map[string]bool)
		blockStateHits10[blockNum] = make(map[string]bool)
		blockStateHits20[blockNum] = make(map[string]bool)
		blockStateHits40[blockNum] = make(map[string]bool)
		blockStateHits80[blockNum] = make(map[string]bool)
		blockStateHits160[blockNum] = make(map[string]bool)

		// 估算该区块访问的状态数量和命中的状态数量
		stateCount := stat.UniqueReads
		hit5Count := int(float64(stateCount) * stat.CacheHitRate5)
		hit10Count := int(float64(stateCount) * stat.CacheHitRate10)
		hit20Count := int(float64(stateCount) * stat.CacheHitRate20)
		hit40Count := int(float64(stateCount) * stat.CacheHitRate40)
		hit80Count := int(float64(stateCount) * stat.CacheHitRate80)
		hit160Count := int(float64(stateCount) * stat.CacheHitRate160)

		// 为每个区块生成唯一的状态ID
		for i := 0; i < stateCount; i++ {
			stateID := fmt.Sprintf("block_%d_state_%d", blockNum, i)

			// 记录访问的状态
			accessedStates[stateID] = true
			blockStateAccess[blockNum][stateID] = true

			// 记录命中缓存的状态
			if i < hit5Count {
				cacheHitStates5[stateID] = true
				blockStateHits5[blockNum][stateID] = true
			}
			if i < hit10Count {
				cacheHitStates10[stateID] = true
				blockStateHits10[blockNum][stateID] = true
			}
			if i < hit20Count {
				cacheHitStates20[stateID] = true
				blockStateHits20[blockNum][stateID] = true
			}
			if i < hit40Count {
				cacheHitStates40[stateID] = true
				blockStateHits40[blockNum][stateID] = true
			}
			if i < hit80Count {
				cacheHitStates80[stateID] = true
				blockStateHits80[blockNum][stateID] = true
			}
			if i < hit160Count {
				cacheHitStates160[stateID] = true
				blockStateHits160[blockNum][stateID] = true
			}
		}
	}

	// 计算整体命中率
	totalAccessedCount := len(accessedStates)
	cacheHitRate5 := float64(len(cacheHitStates5)) / float64(totalAccessedCount)
	cacheHitRate10 := float64(len(cacheHitStates10)) / float64(totalAccessedCount)
	cacheHitRate20 := float64(len(cacheHitStates20)) / float64(totalAccessedCount)
	cacheHitRate40 := float64(len(cacheHitStates40)) / float64(totalAccessedCount)
	cacheHitRate80 := float64(len(cacheHitStates80)) / float64(totalAccessedCount)
	cacheHitRate160 := float64(len(cacheHitStates160)) / float64(totalAccessedCount)

	// 计算基于区块的平均值
	count := float64(len(recentStats))
	avgProcessTime := time.Duration(float64(totalProcessTime) / count)
	avgRootGenTime := time.Duration(float64(totalRootGenTime) / count)
	avgTotalTime := time.Duration(float64(totalTotalTime) / count)
	// 只计算处理和根哈希的时间百分比
	avgProcessPercent := float64(totalProcessTime) / float64(totalProcessTime+totalRootGenTime) * 100
	avgRootGenPercent := float64(totalRootGenTime) / float64(totalProcessTime+totalRootGenTime) * 100

	avgTxCount := float64(totalTxCount) / count
	avgSuccessRate := totalSuccessRate / count
	avgContractTxPercent := totalContractTxPercent / count
	avgContractSuccessRate := float64(totalContractSuccessCount) / float64(totalContractTxCount)
	avgStatesPerTx := totalAvgStatesPerTx / count
	avgStatesPerContractTx := totalAvgStatesPerContractTx / count
	avgUniqueReads := float64(totalUniqueReads) / count
	avgUniqueWrites := float64(totalUniqueWrites) / count

	fmt.Printf("===== 区块统计 (区块范围: %d - %d) =====\n",
		recentStats[0].BlockNum, recentStats[len(recentStats)-1].BlockNum)
	fmt.Printf("处理区块数: %d, 总交易数: %d, 成功交易数: %d, 成功率: %.2f%%\n",
		len(recentStats), totalTxCount, totalSuccessCount, avgSuccessRate*100)
	fmt.Printf("合约交易: %d (%.2f%%), 成功合约交易: %d, 合约成功率: %.2f%%\n",
		totalContractTxCount, avgContractTxPercent*100, totalContractSuccessCount, avgContractSuccessRate*100)
	fmt.Printf("交易数 - 平均: %.1f, 最大: %d\n",
		avgTxCount, maxTxCount)

	// 添加错误统计信息
	fmt.Println("\n----- 合约错误统计 -----")

	// 合并所有区块的错误计数
	allErrorReasons := make(map[string]int)
	allCreateErrorReasons := make(map[string]int)
	allCallErrorReasons := make(map[string]int)
	totalErrors := 0

	for _, stat := range recentStats {
		totalErrors += stat.ErrorCount

		// 合并错误原因计数
		if stat.ErrorReasons != nil {
			for reason, count := range stat.ErrorReasons {
				allErrorReasons[reason] += count
			}
		}
		if stat.CreateErrorReasons != nil {
			for reason, count := range stat.CreateErrorReasons {
				allCreateErrorReasons[reason] += count
			}
		}
		if stat.CallErrorReasons != nil {
			for reason, count := range stat.CallErrorReasons {
				allCallErrorReasons[reason] += count
			}
		}
	}

	// 打印错误统计
	fmt.Printf("总错误数: %d (%.2f%% 的交易)\n",
		totalErrors, float64(totalErrors)/float64(totalTxCount)*100)

	// 错误原因排序
	type ErrorCount struct {
		Reason string
		Count  int
	}

	// 对所有错误进行排序
	allErrorCounts := make([]ErrorCount, 0, len(allErrorReasons))
	for reason, count := range allErrorReasons {
		allErrorCounts = append(allErrorCounts, ErrorCount{reason, count})
	}
	sort.Slice(allErrorCounts, func(i, j int) bool {
		return allErrorCounts[i].Count > allErrorCounts[j].Count
	})

	// 打印总体错误类型分布
	fmt.Println("\n常见错误原因 (全部):")
	var totalPrinted int
	for i, ec := range allErrorCounts {
		if i >= 10 && float64(ec.Count)/float64(totalErrors) < 0.01 {
			// 只打印前10个错误和占比超过1%的错误
			break
		}
		fmt.Printf("  %-30s: %d (%.2f%%)\n",
			ec.Reason, ec.Count, float64(ec.Count)/float64(totalErrors)*100)
		totalPrinted += ec.Count
	}

	// 如果还有其他错误未打印
	if totalPrinted < totalErrors {
		fmt.Printf("  %-30s: %d (%.2f%%)\n",
			"其他错误", totalErrors-totalPrinted,
			float64(totalErrors-totalPrinted)/float64(totalErrors)*100)
	}

	// 打印合约创建错误
	createErrorCounts := make([]ErrorCount, 0, len(allCreateErrorReasons))
	totalCreateErrors := 0
	for reason, count := range allCreateErrorReasons {
		createErrorCounts = append(createErrorCounts, ErrorCount{reason, count})
		totalCreateErrors += count
	}
	sort.Slice(createErrorCounts, func(i, j int) bool {
		return createErrorCounts[i].Count > createErrorCounts[j].Count
	})

	if totalCreateErrors > 0 {
		fmt.Println("\n合约创建错误原因:")
		for i, ec := range createErrorCounts {
			if i >= 5 && float64(ec.Count)/float64(totalCreateErrors) < 0.05 {
				// 只打印前5个错误和占比超过5%的错误
				break
			}
			fmt.Printf("  %-30s: %d (%.2f%%)\n",
				ec.Reason, ec.Count, float64(ec.Count)/float64(totalCreateErrors)*100)
		}
	}

	// 打印合约调用错误
	callErrorCounts := make([]ErrorCount, 0, len(allCallErrorReasons))
	totalCallErrors := 0
	for reason, count := range allCallErrorReasons {
		callErrorCounts = append(callErrorCounts, ErrorCount{reason, count})
		totalCallErrors += count
	}
	sort.Slice(callErrorCounts, func(i, j int) bool {
		return callErrorCounts[i].Count > callErrorCounts[j].Count
	})

	if totalCallErrors > 0 {
		fmt.Println("\n合约调用错误原因:")
		for i, ec := range callErrorCounts {
			if i >= 5 && float64(ec.Count)/float64(totalCallErrors) < 0.05 {
				// 只打印前5个错误和占比超过5%的错误
				break
			}
			fmt.Printf("  %-30s: %d (%.2f%%)\n",
				ec.Reason, ec.Count, float64(ec.Count)/float64(totalCallErrors)*100)
		}
	}

	fmt.Println("\n----- 性能统计 -----")
	fmt.Printf("唯一状态访问 - 读(平均/最大): %.1f/%d, 写(平均/最大): %.1f/%d\n",
		avgUniqueReads, maxUniqueReads, avgUniqueWrites, maxUniqueWrites)
	fmt.Printf("每交易状态访问 - 所有交易: %.2f, 合约交易: %.2f\n",
		avgStatesPerTx, avgStatesPerContractTx)
	fmt.Printf("平均时间 - 交易处理: %v (%.1f%%), 根哈希: %v (%.1f%%), 总计: %v\n",
		avgProcessTime, avgProcessPercent, avgRootGenTime, avgRootGenPercent, avgTotalTime)
	fmt.Printf("最大时间 - 交易处理: %v, 根哈希: %v, 总计: %v\n",
		maxProcessTime, maxRootGenTime, maxTotalTime)
	fmt.Printf("缓存命中率 - 总状态数: %d, 5区块(%.1f%%), 10区块(%.1f%%), 20区块(%.1f%%), 40区块(%.1f%%), 80区块(%.1f%%), 160区块(%.1f%%)\n",
		totalAccessedCount, cacheHitRate5*100, cacheHitRate10*100, cacheHitRate20*100, cacheHitRate40*100, cacheHitRate80*100, cacheHitRate160*100)
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
		"BlockNum", "TransactionCount", "SuccessCount", "SuccessRate",
		"ContractTxCount", "ContractTxPercent", "ContractSuccessCount", "ContractSuccessRate",
		"ProcessTime(ms)", "RootGenTime(ms)", "TotalTime(ms)",
		"ProcessPercent", "RootGenPercent",
		"UniqueReads", "UniqueWrites",
		"AvgStatesPerTx", "AvgStatesPerContractTx",
		"HitRate5", "HitRate10", "HitRate20", "HitRate40", "HitRate80", "HitRate160",
		"ErrorCount", "TopErrorReason", "TopErrorCount", // 添加错误统计字段
	}
	writer.Write(headers)

	// 写入每个区块的统计数据
	for _, stat := range s.Stats {
		// 找出最常见的错误原因
		var topErrorReason string
		var topErrorCount int
		if stat.ErrorReasons != nil {
			for reason, count := range stat.ErrorReasons {
				if count > topErrorCount {
					topErrorReason = reason
					topErrorCount = count
				}
			}
		}

		record := []string{
			strconv.FormatUint(stat.BlockNum, 10),
			strconv.Itoa(stat.TransactionCount),
			strconv.Itoa(stat.SuccessCount),
			strconv.FormatFloat(stat.SuccessRate, 'f', 4, 64),
			strconv.Itoa(stat.ContractTxCount),
			strconv.FormatFloat(stat.ContractTxPercent, 'f', 4, 64),
			strconv.Itoa(stat.ContractSuccessCount),
			strconv.FormatFloat(stat.ContractSuccessRate, 'f', 4, 64),
			strconv.FormatInt(stat.ProcessTime.Milliseconds(), 10),
			strconv.FormatInt(stat.RootGenTime.Milliseconds(), 10),
			strconv.FormatInt(stat.TotalTime.Milliseconds(), 10),
			strconv.FormatFloat(stat.ProcessTimePercent, 'f', 2, 64),
			strconv.FormatFloat(stat.RootGenTimePercent, 'f', 2, 64),
			strconv.Itoa(stat.UniqueReads),
			strconv.Itoa(stat.UniqueWrites),
			strconv.FormatFloat(stat.AvgStatesPerTx, 'f', 4, 64),
			strconv.FormatFloat(stat.AvgStatesPerContractTx, 'f', 4, 64),
			strconv.FormatFloat(stat.CacheHitRate5, 'f', 4, 64),
			strconv.FormatFloat(stat.CacheHitRate10, 'f', 4, 64),
			strconv.FormatFloat(stat.CacheHitRate20, 'f', 4, 64),
			strconv.FormatFloat(stat.CacheHitRate40, 'f', 4, 64),
			strconv.FormatFloat(stat.CacheHitRate80, 'f', 4, 64),
			strconv.FormatFloat(stat.CacheHitRate160, 'f', 4, 64),
			strconv.Itoa(stat.ErrorCount),
			topErrorReason,
			strconv.Itoa(topErrorCount),
		}
		writer.Write(record)
	}

	fmt.Printf("已输出CSV文件: %s\n", filename)
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

	// 创建或打开持久化数据库
	ldb, err := leveldb.New(dbDir, 1024, 1024, "eth-process-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	//cacheConfig := DefaultCacheConfigWithScheme(rawdb.PathScheme)
	db := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(db, &triedb.Config{
		Preimages: false,
		IsVerkle:  common.UserVerkle,
		CacheTrie: common.UseCacheTrie,
		ReadCache: false,
		StartNum:  startNum,
		HashDB:    hashdb.Defaults,
	})
	var snaps *snapshot.Tree
	snaps, _ = snapshot.New(snapshot.Config{CacheSize: 100}, db, trieDB, types.EmptyRootHash)
	sdb := state.NewDatabase(trieDB, snaps)
	var preTrieDB *triedb.Database
	var preSdb *state.CachingDB
	if common.UseCacheTrie {
		preTrieDB = triedb.NewDatabase(db, &triedb.Config{
			Preimages: false,
			IsVerkle:  common.UserVerkle,
			CacheTrie: false,
			ReadCache: false,
			StartNum:  startNum,
			HashDB:    hashdb.Defaults,
		})
		preSdb = state.NewDatabase(preTrieDB, snaps)
	}

	// 创建genesis区块和区块链
	gspec := &Genesis{
		Config: params.MainnetChainConfig,
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
	counter := NewStateAccessCounter(160) // 记录最近50个区块

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

					if isContractCreate && strings.Contains(result.Err.Error(), "out of gas") {
						t.Logf("find a fail contract : %v, err: %v, limit: %v, gas: %v.", result.ContractAddress, errReason, msg.GasLimit, result.UsedGas)
					}

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
			var commitDuration time.Duration
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
				// 记录deleteKVList以便在处理中使用
				deleteKVList := countingStateDB.GetCachedDeleteKVList()
				go func(root common.Hash, sBlockNum uint64, deleteKVList *cacheTrie.DeleteKVList) {
					commitStart := time.Now()
					if deleteKVList == nil {
						return
					}
					if len(deleteKVList.Data) == 0 {
						trieDB.CacheTrie().FinishCleanup(sBlockNum, root)
						return
					}
					// 使用当前状态根创建新的stateDB
					cleanStateDB, err := state.New(root, preSdb)

					// 第二步：处理所有账户
					for _, kv := range deleteKVList.Data {
						// 地址为空且键存在，说明是账户
						if (kv.Address == common.Address{}) && len(kv.Key) > 0 {
							addr := common.BytesToAddress(kv.Key)
							cleanStateDB.SetAccount(addr, kv.Value, 0)
						}
					}

					// 第一步：处理所有状态（存储槽）
					for _, kv := range deleteKVList.Data {
						// 通过Address区分是否有地址，如果地址非空，则是存储槽
						if (kv.Address != common.Address{}) && len(kv.Key) > 0 {
							// 有地址且有键，说明是存储槽
							addr := kv.Address
							key := common.BytesToHash(kv.Key)
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
					newRoot, err := cleanStateDB.Commit(sBlockNum, false, false)
					if err != nil {
						t.Fatalf("提交无cache stateDB失败: %v", err)
					}

					// 第四步：将结果提交到数据库
					err = preTrieDB.Commit(newRoot, false)
					if err != nil {
						t.Fatalf("提交trieDB失败: %v", err)
					}

					trieDB.CacheTrie().FinishCleanup(sBlockNum, newRoot)
					commitDuration = time.Since(commitStart)

					if err != nil {
						t.Fatalf("提交状态失败，区块 %d: %v", sBlockNum, err)
					}

					// 刷新数据库，避免内存占用过大
					preTrieDB.Cap(1024 * 1024 * 1024) // 1GB内存限制

				}(root, blockNum, deleteKVList)
			} else {
				root, _ = countingStateDB.Commit(blockNum, false, false)
				// 提交状态到数据库阶段 - 只在达到配置的间隔时才提交
				if blockNum-lastCommitBlock >= 1000 { // 每1000个区块提交一次
					commitStart := time.Now()
					err = trieDB.Commit(root, false)
					commitDuration = time.Since(commitStart)
					lastCommitBlock = blockNum

					if err != nil {
						t.Fatalf("提交状态失败，区块 %d: %v", blockNum, err)
					}

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

			// 获取缓存命中率
			hitRate5 := counter.GetCacheHitRate(5)
			hitRate10 := counter.GetCacheHitRate(10)
			hitRate20 := counter.GetCacheHitRate(20)
			hitRate40 := counter.GetCacheHitRate(40)
			hitRate80 := counter.GetCacheHitRate(80)
			hitRate160 := counter.GetCacheHitRate(160)

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

			// 输出该区块的详细状态访问统计
			//counter.OutputAccessStats()

			// 计算每个交易平均状态访问数
			avgStatesPerTx := 0.0
			if len(msgsByBlock[blockNum]) > 0 {
				totalStates := counter.UniqueReads + counter.UniqueWrites
				avgStatesPerTx = float64(totalStates) / float64(len(msgsByBlock[blockNum]))
			}

			// 计算每个合约交易平均状态访问数
			avgStatesPerContractTx := 0.0
			if contractTxCount > 0 {
				totalStates := counter.UniqueReads + counter.UniqueWrites
				avgStatesPerContractTx = float64(totalStates) / float64(contractTxCount)
			}

			// 计算每个合约创建交易平均状态访问数
			avgStatesPerCreateTx := 0.0
			if createContractCount > 0 {
				totalStates := counter.UniqueReads + counter.UniqueWrites
				avgStatesPerCreateTx = float64(totalStates) / float64(createContractCount)
			}

			// 计算每个合约调用交易平均状态访问数
			avgStatesPerCallTx := 0.0
			if callContractCount > 0 {
				totalStates := counter.UniqueReads + counter.UniqueWrites
				avgStatesPerCallTx = float64(totalStates) / float64(callContractCount)
			}

			// 创建区块统计数据
			blockStats := BlockStats{
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

				// 错误统计（新增）
				ErrorCount:         errorCount,
				ErrorReasons:       errorReasons,
				CreateErrorReasons: createErrorReasons,
				CallErrorReasons:   callErrorReasons,

				ProcessTime:            processDuration,
				RootGenTime:            rootGenDuration,
				CommitTime:             commitDuration,
				TotalTime:              totalTime,
				ProcessTimePercent:     processPercent,
				RootGenTimePercent:     rootGenPercent,
				UniqueReads:            counter.UniqueReads,
				UniqueWrites:           counter.UniqueWrites,
				AvgStatesPerTx:         avgStatesPerTx,
				AvgStatesPerContractTx: avgStatesPerContractTx,
				AvgStatesPerCreateTx:   avgStatesPerCreateTx,
				AvgStatesPerCallTx:     avgStatesPerCallTx,
				CacheHitRate5:          hitRate5,
				CacheHitRate10:         hitRate10,
				CacheHitRate20:         hitRate20,
				CacheHitRate40:         hitRate40,
				CacheHitRate80:         hitRate80,
				CacheHitRate160:        hitRate160,
			}

			// 添加到统计聚合器
			statsAgg.AddBlockStats(blockStats)
		}
		t.Logf("完成处理文件: %s", file)
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

// OutputAccessStats 输出状态访问统计
func (c *StateAccessCounter) OutputAccessStats() {
	if c.DetailedAccessLog {
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

		fmt.Printf("\n## 整体访问统计 ##\n")
		fmt.Printf("只读状态: %d (%.2f%%)\n",
			len(c.ReadsOnly), float64(len(c.ReadsOnly))*100/float64(c.UniqueReads+c.UniqueWrites))
		fmt.Printf("读后写状态: %d (%.2f%%)\n",
			len(c.ReadThenWritten), float64(len(c.ReadThenWritten))*100/float64(c.UniqueReads+c.UniqueWrites))
		fmt.Printf("只写状态: %d (%.2f%%)\n",
			len(c.WritesOnly), float64(len(c.WritesOnly))*100/float64(c.UniqueReads+c.UniqueWrites))

		// 添加对只写状态的分析，这应该主要是存储写入
		if len(c.WritesOnly) > 0 {
			storageWriteOnlyPercent := float64(storageWriteOnly) * 100 / float64(len(c.WritesOnly))
			fmt.Printf("只写状态中存储写入: %d (%.2f%%)\n",
				storageWriteOnly, storageWriteOnlyPercent)
		}

		// 输出前10个只读状态的键
		if len(c.ReadsOnly) > 0 {
			fmt.Printf("\n前10个只读状态示例:\n")
			i := 0
			for key := range c.ReadsOnly {
				fmt.Printf("  %s\n", key)
				i++
				if i >= 10 {
					break
				}
			}
		}

		// 输出前10个读后写状态的键
		if len(c.ReadThenWritten) > 0 {
			fmt.Printf("\n前10个读后写状态示例:\n")
			i := 0
			for key := range c.ReadThenWritten {
				fmt.Printf("  %s\n", key)
				i++
				if i >= 10 {
					break
				}
			}
		}

		// 输出前10个只写状态的键
		if len(c.WritesOnly) > 0 {
			fmt.Printf("\n前10个只写状态示例:\n")
			i := 0
			for key := range c.WritesOnly {
				fmt.Printf("  %s\n", key)
				i++
				if i >= 10 {
					break
				}
			}
		}

		fmt.Println("======================================")
	}
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

// ProcessDeleteKVList 处理缓存中的deleteKVList
// 创建一个无cache的新stateDB，将deleteKV数据写入，然后commit
// 这种方案对原有代码侵入改动较小
func ProcessDeleteKVList(stateDB *state.StateDB, blockNum uint64) error {

	return nil
}
