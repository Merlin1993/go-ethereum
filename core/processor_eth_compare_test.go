package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/triedb/hashdb"
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

// CompareStateAccessCounter 用于记录状态访问信息
type CompareStateAccessCounter struct {
	BlockNum uint64 // 当前区块号

	// 跟踪已经访问过的状态，只记录统计信息
	VisitedReads  map[string]bool // 本区块内已经读取过的状态
	VisitedWrites map[string]bool // 本区块内已经写入过的状态
	UniqueReads   int             // 区块内唯一的读操作数量
	UniqueWrites  int             // 区块内唯一的写操作数量

	// 记录所有部署的合约地址
	ContractAddresses map[common.Address]bool // 记录所有部署的合约地址
}

// NewCompareStateAccessCounter 创建一个新的状态访问计数器
func NewCompareStateAccessCounter() *CompareStateAccessCounter {
	return &CompareStateAccessCounter{
		VisitedReads:      make(map[string]bool),
		VisitedWrites:     make(map[string]bool),
		ContractAddresses: make(map[common.Address]bool),
	}
}

// RecordStateRead 记录状态读取
func (c *CompareStateAccessCounter) RecordStateRead(key string) {
	// 如果该键尚未被读取过且尚未被写入过，增加唯一读取计数
	if !c.VisitedReads[key] && !c.VisitedWrites[key] {
		c.UniqueReads++
		c.VisitedReads[key] = true
	}
}

// RecordStateWrite 记录状态写入
func (c *CompareStateAccessCounter) RecordStateWrite(key string) {
	// 如果该键尚未被写入过，增加唯一写入计数
	if !c.VisitedWrites[key] {
		c.UniqueWrites++
		c.VisitedWrites[key] = true
	}
}

// NextBlock 进入下一个区块，清除旧的状态记录
func (c *CompareStateAccessCounter) NextBlock(blockNum uint64) {
	c.BlockNum = blockNum
	c.VisitedReads = make(map[string]bool)
	c.VisitedWrites = make(map[string]bool)
	c.UniqueReads = 0
	c.UniqueWrites = 0
}

// GetAllAccessedStateKeys 返回所有被访问过的状态键
func (c *CompareStateAccessCounter) GetAllAccessedStateKeys() map[string]bool {
	// 合并读写状态键
	allKeys := make(map[string]bool)
	for key := range c.VisitedReads {
		allKeys[key] = true
	}
	for key := range c.VisitedWrites {
		allKeys[key] = true
	}
	return allKeys
}

// 区块统计数据结构
type CompareBlockStats struct {
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

	// 简化的错误统计
	ErrorCount int     // 错误总数
	ErrorRate  float64 // 错误率（占总交易的百分比）

	ProcessTime        time.Duration // 交易处理时间
	RootGenTime        time.Duration // 根哈希生成时间
	CommitTime         time.Duration // 数据库提交时间
	TotalTime          time.Duration // 总时间
	ProcessTimePercent float64       // 处理时间占比
	RootGenTimePercent float64       // 根哈希时间占比

	UniqueReads  int // 唯一读状态数
	UniqueWrites int // 唯一写状态数
}

// 统计聚合结构
type CompareStatsAggregator struct {
	Stats                []CompareBlockStats // 所有区块的统计数据
	OutputDir            string              // 输出目录
	BlockWindow          uint64              // 统计窗口大小(每隔多少区块打印一次)
	LastOutputBlock      uint64              // 上次输出统计的区块号
	TotalProcessed       int                 // 总处理区块数
	TotalTransaction     int                 // 总交易数
	TotalSuccess         int                 // 总成功交易数
	TotalContractTx      int                 // 总合约交易数
	TotalContractSuccess int                 // 总成功的合约交易数
}

// 创建新的统计聚合器
func NewCompareStatsAggregator(outputDir string, blockWindow uint64) *CompareStatsAggregator {
	// 确保输出目录存在
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		os.MkdirAll(outputDir, 0755)
	}

	return &CompareStatsAggregator{
		Stats:                make([]CompareBlockStats, 0),
		OutputDir:            outputDir,
		BlockWindow:          blockWindow,
		LastOutputBlock:      0,
		TotalProcessed:       0,
		TotalTransaction:     0,
		TotalSuccess:         0,
		TotalContractTx:      0,
		TotalContractSuccess: 0,
	}
}

// 添加区块统计数据
func (s *CompareStatsAggregator) AddBlockStats(stats CompareBlockStats) {
	s.Stats = append(s.Stats, stats)
	s.TotalProcessed++
	s.TotalTransaction += stats.TransactionCount
	s.TotalSuccess += stats.SuccessCount
	s.TotalContractTx += stats.ContractTxCount
	s.TotalContractSuccess += stats.ContractSuccessCount

	// 检查是否需要打印统计信息
	if stats.BlockNum-s.LastOutputBlock >= s.BlockWindow {
		s.PrintStats()
		s.LastOutputBlock = stats.BlockNum
	}
}

// 打印统计信息
func (s *CompareStatsAggregator) PrintStats() {
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
	var totalTxCount, totalSuccessCount, totalContractTxCount, totalContractSuccessCount int
	var totalCreateContractCount, totalCreateSuccessCount int
	var totalCallContractCount, totalCallSuccessCount int

	// 交易统计
	var totalSuccessRate, totalContractTxPercent, totalContractSuccessRate float64

	for _, stat := range recentStats {
		totalTxCount += stat.TransactionCount
		totalSuccessCount += stat.SuccessCount
		totalContractTxCount += stat.ContractTxCount
		totalContractSuccessCount += stat.ContractSuccessCount
		totalCreateContractCount += stat.CreateContractCount
		totalCreateSuccessCount += stat.CreateSuccessCount
		totalCallContractCount += stat.CallContractCount
		totalCallSuccessCount += stat.CallSuccessCount

		totalSuccessRate += stat.SuccessRate
		totalContractTxPercent += stat.ContractTxPercent
		totalContractSuccessRate += stat.ContractSuccessRate
	}

	// 计算基于区块的平均值
	count := float64(len(recentStats))
	avgSuccessRate := totalSuccessRate / count
	avgContractTxPercent := totalContractTxPercent / count
	avgContractSuccessRate := float64(totalContractSuccessCount) / float64(totalContractTxCount)

	// 计算创建合约和调用合约的成功率
	avgCreateSuccessRate := 0.0
	if totalCreateContractCount > 0 {
		avgCreateSuccessRate = float64(totalCreateSuccessCount) / float64(totalCreateContractCount) * 100
	}

	avgCallSuccessRate := 0.0
	if totalCallContractCount > 0 {
		avgCallSuccessRate = float64(totalCallSuccessCount) / float64(totalCallContractCount) * 100
	}

	if !common.DebugFlag {
		return
	}
	fmt.Printf("===== [对比测试] 区块统计 (区块范围: %d - %d) =====\n",
		recentStats[0].BlockNum, recentStats[len(recentStats)-1].BlockNum)
	fmt.Printf("处理区块数: %d, 总交易数: %d, 成功交易数: %d, 成功率: %.2f%%\n",
		len(recentStats), totalTxCount, totalSuccessCount, avgSuccessRate*100)
	fmt.Printf("合约交易: %d (%.2f%%), 成功合约交易: %d, 合约成功率: %.2f%%\n",
		totalContractTxCount, avgContractTxPercent*100, totalContractSuccessCount, avgContractSuccessRate*100)
	fmt.Printf("合约创建: %d, 成功: %d, 成功率: %.2f%%\n",
		totalCreateContractCount, totalCreateSuccessCount, avgCreateSuccessRate)
	fmt.Printf("合约调用: %d, 成功: %d, 成功率: %.2f%%\n",
		totalCallContractCount, totalCallSuccessCount, avgCallSuccessRate)
	fmt.Println("=======================================")
}

// 处理配置
type CompareProcessConfig struct {
	CommitInterval uint64 // 每处理多少个区块提交一次，默认1000
}

// 默认配置
func DefaultCompareProcessConfig() CompareProcessConfig {
	return CompareProcessConfig{
		CommitInterval: 1000,
	}
}

// 自然数字排序函数，保证文件按照数字顺序排序（如：1, 2, ..., 10, 11，而不是1, 10, 11, 2, ...）
func compareNaturalSort(files []string) {
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
var compareBlockTimestamps = make(map[uint64]uint64)

// 从对应的区块文件中加载时间戳
func compareLoadBlockTimestampsFromFile(dataDir string, fileIndex string) error {
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
		compareBlockTimestamps[num] = timestamp
	}

	return nil
}

// 获取交易文件的索引部分
func compareGetFileIndex(filePath string) string {
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

// 查找所有匹配的CSV文件并按顺序排序
func compareFindTransactionFiles(dataDir string) ([]string, error) {
	// 使用通配符匹配所有transactions_*.csv文件
	pattern := filepath.Join(dataDir, "transactions_*.csv")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("查找CSV文件失败: %v", err)
	}

	// 按自然数排序
	compareNaturalSort(files)
	return files, nil
}

// TestCompareProcessTransactions 测试处理CSV中的交易
func TestCompareProcessTransactions(t *testing.T) {
	// 定义数据库路径
	dbDir := "F:\\ethdata\\geth_compare_db_swmt"
	statsDir := "F:\\ethdata\\compare_stats"
	dataDir := "E:\\ethdata"

	// 指定文件范围，硬编码方式指定起始和结束文件索引
	startFileIdx := 1 // 起始文件索引（从1开始）
	endFileIdx := 7   // 结束文件索引
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
	gspec := &Genesis{
		Config: params.TestChainConfig,
		Alloc:  GenesisAlloc{},
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

			// 为所有发送方预分配余额
			for _, msg := range msgsByBlock[blockNum] {
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

			// 错误统计（简化）
			var errorCount int

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

					//fmt.Println(fmt.Sprintf("start clean  root : %v", root.String()))

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
					newRoot, err := cleanStateDB.Commit(sBlockNum, false, false)
					if err != nil {
						t.Fatalf("提交无cache stateDB失败: %v, cHash : %v", err, cHash)
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

				rootGenDuration = time.Since(rootGenStart)
				// 提交状态到数据库阶段 - 只在达到配置的间隔时才提交
				//todo 测试是不是这个导致的
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

			//if blockNum%20000 == 0 {
			//	state.PrintStat()
			//}

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

// CompareCountingStateDB 是CountingStateDB类似的结构，用于拦截状态访问
type CompareCountingStateDB struct {
	*state.StateDB
	counter *CompareStateAccessCounter
}

// GetState 重写GetState方法，增加计数
func (db *CompareCountingStateDB) GetState(addr common.Address, key common.Hash) common.Hash {
	// 记录对存储槽位的读取

	stateKey := addr.Hex() + ":" + key.Hex()
	db.counter.RecordStateRead(stateKey)

	// 同时记录对账户地址的读取
	addrStr := addr.Hex()
	db.counter.RecordStateRead(addrStr)
	//if addr.Hex() == "0xcd134CE565e6b7f7CEfE2122A07A2e56D6ECbB26" && key.Hex() == "0x0000000000000000000000000000000000000000000000000000000000000105" {
	//	addr.Hex()
	//}

	value := db.StateDB.GetState(addr, key)
	//if addr.Hex() == "0xcd134CE565e6b7f7CEfE2122A07A2e56D6ECbB26" && key.Hex() == "0x0000000000000000000000000000000000000000000000000000000000000105" {
	//	fmt.Printf("warning read: %v , value ： %v \n", db.counter.BlockNum, value.String())
	//}
	//if db.counter.BlockNum == 767443 {
	//	fmt.Printf("addresss %v 读取：%v ： %v \n", addr, key, value)
	//}
	return value
}

// SetState 重写SetState方法，增加计数
func (db *CompareCountingStateDB) SetState(addr common.Address, key, value common.Hash) common.Hash {
	// 记录对存储槽位的写入
	stateKey := addr.Hex() + ":" + key.Hex()
	//if addr.Hex() == "0xcd134CE565e6b7f7CEfE2122A07A2e56D6ECbB26" && key.Hex() == "0x0000000000000000000000000000000000000000000000000000000000000105" {
	//	fmt.Printf("warning write: %v , value ： %v \n", db.counter.BlockNum, value.String())
	//}
	db.counter.RecordStateWrite(stateKey)

	// 同时记录对账户地址的写入
	addrStr := addr.Hex()
	db.counter.RecordStateWrite(addrStr)
	//if addr.Hex() == "0xcd134CE565e6b7f7CEfE2122A07A2e56D6ECbB26" && key.Hex() == "0x0000000000000000000000000000000000000000000000000000000000000105" {
	//	addr.Hex()
	//}

	//if db.counter.BlockNum == 767443 {
	//	fmt.Printf("addresss %v 写入：%v ： %v \n", addr, key, value)
	//}
	return db.StateDB.SetState(addr, key, value)
}

// GetBalance 重写GetBalance方法，增加计数
func (db *CompareCountingStateDB) GetBalance(addr common.Address) *uint256.Int {
	addrStr := addr.Hex()
	// GetBalance实际上是GetStateObject的时候对账户的访问
	db.counter.RecordStateRead(addrStr)
	return db.StateDB.GetBalance(addr)
}

// SetBalance 重写SetBalance方法，增加计数
func (db *CompareCountingStateDB) SetBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) {
	addrStr := addr.Hex()
	// SetBalance也会先读取账户，然后更新
	db.counter.RecordStateRead(addrStr)
	db.counter.RecordStateWrite(addrStr)
	db.StateDB.SetBalance(addr, amount, reason)
}

// GetNonce 重写GetNonce方法，增加计数
func (db *CompareCountingStateDB) GetNonce(addr common.Address) uint64 {
	addrStr := addr.Hex()
	// GetNonce实际上是GetStateObject的时候对账户的访问
	db.counter.RecordStateRead(addrStr)
	return db.StateDB.GetNonce(addr)
}

// SetNonce 重写SetNonce方法，增加计数
func (db *CompareCountingStateDB) SetNonce(addr common.Address, nonce uint64, reason tracing.NonceChangeReason) {
	addrStr := addr.Hex()
	// SetNonce会先读取账户，然后更新
	db.counter.RecordStateRead(addrStr)
	db.counter.RecordStateWrite(addrStr)
	db.StateDB.SetNonce(addr, nonce, reason)
}

// SubBalance 重写SubBalance方法，增加计数
func (db *CompareCountingStateDB) SubBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	addrStr := addr.Hex()
	// SubBalance需要先读取当前余额
	db.counter.RecordStateRead(addrStr)
	db.counter.RecordStateWrite(addrStr)
	return db.StateDB.SubBalance(addr, amount, reason)
}

// AddBalance 重写AddBalance方法，增加计数
func (db *CompareCountingStateDB) AddBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	addrStr := addr.Hex()
	// AddBalance需要先读取当前余额
	db.counter.RecordStateRead(addrStr)
	db.counter.RecordStateWrite(addrStr)
	return db.StateDB.AddBalance(addr, amount, reason)
}

// 确保CompareCountingStateDB实现了vm.StateDB接口
var _ vm.StateDB = (*CompareCountingStateDB)(nil)

// TestCompareVerkleProcessTransactions 测试处理CSV中的交易
func TestCompareVerkleProcessTransactions(t *testing.T) {
	// 定义数据库路径
	dbDir := "F:\\ethdata\\geth_compare_db_verkle_swmt"
	statsDir := "F:\\ethdata\\compare_stats_swmt"
	dataDir := "E:\\ethdata"

	// 指定文件范围，硬编码方式指定起始和结束文件索引
	startFileIdx := 1 // 起始文件索引（从1开始）
	endFileIdx := 7   // 结束文件索引
	//46147
	var startNum uint64 = 46147

	// 创建统计聚合器
	statsAgg := NewCompareStatsAggregator(statsDir, 100000) // 使用直接数值替代常量

	// 添加: 创建状态树统计记录器
	trieStatsDir := filepath.Join(statsDir, "trie_stats_swmt")
	cacheTrieRecorder := CreateTrieStatsRecorder(trieStatsDir, dbDir, CacheTrie)

	// 创建或打开持久化数据库
	ldb, err := leveldb.New(dbDir, 1024, 1024, "eth-compare-process-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	db := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(db, &triedb.Config{
		Preimages: false,
		IsVerkle:  true,
		CacheTrie: true,
		ReadCache: false,
		StartNum:  startNum,
		PathDB:    pathdb.Defaults,
		//HashDB: hashdb.Defaults,
	})

	// 使用正确的state包API
	var snaps *snapshot.Tree
	snaps, _ = snapshot.New(snapshot.Config{CacheSize: 100}, db, trieDB, common.Hash{})
	sdb := state.NewDatabase(trieDB, snaps)
	var preTrieDB *triedb.Database
	var preSdb *state.CachingDB

	//preTrieDB = triedb.NewDatabase(db, &triedb.Config{
	//	Preimages: false,
	//	IsVerkle:  true,
	//	CacheTrie: false,
	//	ReadCache: false,
	//	StartNum:  startNum,
	//	PathDB:    pathdb.Defaults,
	//})
	preTrieDB = triedb.NewDatabase2(db, &triedb.Config{
		Preimages: false,
		IsVerkle:  true,
		CacheTrie: false,
		ReadCache: false,
		StartNum:  startNum,
		PathDB:    pathdb.Defaults,
	}, trieDB.GetBackend())

	preSdb = state.NewDatabase(preTrieDB, snaps)

	// 创建genesis区块和区块链
	gspec := &Genesis{
		Config: params.TestChainConfig,
		Alloc:  GenesisAlloc{},
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

			// 为所有发送方预分配余额
			for _, msg := range msgsByBlock[blockNum] {
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

			// 错误统计（简化）
			var errorCount int

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
			root := lastStateRoot
			var cHash common.Hash
			var resultHash common.Hash
			cHash, resultHash, _ = countingStateDB.PreCommit(false)
			rootGenDuration := time.Since(rootGenStart)
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

				if err != nil {
					panic(err)
				}
				//fmt.Println(fmt.Sprintf("start clean  root : %v", root.String()))

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
				newRoot, err := cleanStateDB.Commit(sBlockNum, false, false)
				if err != nil {
					t.Fatalf("提交无cache stateDB失败: %v, cHash : %v", err, cHash)
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

			RecordCacheTrieStats(cacheTrieRecorder, blockNum, counter.UniqueWrites, counter.UniqueReads,
				len(msgsByBlock[blockNum]), processDuration, rootGenDuration, ct)
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
