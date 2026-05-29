package tree

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/holiman/uint256"
)

type silentReadStatsSnapshot struct {
	totalReads                 int64
	totalAccountReads          int64
	cacheAccountHit            int64
	cacheAccountMissNotExists  int64
	cacheAccountMissExists     int64
	binaryHitCount             int64
	binaryMissNonExistentCount int64
	binaryMissExistentCount    int64
	binaryCycleFPCount         int64
	binaryMaxFPInSingleBlock   int64
	binaryTrieFPInBlock        int64
	binaryProofVerifTime       int64
	binaryProofVerifTimeMax    int64
	binaryProofGenTime         int64
	binaryProofGenTimeMax      int64
	binaryBlockProofSize       int64
	binaryBlockProofSizeMax    int64
	binaryTotalProofSize       int64
	binaryItemProofSizeMin     int64
	binaryItemProofSizeMax     int64
	binaryItemProofSizesLen    int
	binaryFPDistributionLen    int
}

func captureSilentReadStats() silentReadStatsSnapshot {
	common.BinaryStatsMu.Lock()
	itemProofSizesLen := len(common.BinaryItemProofSizes)
	fpDistributionLen := len(common.BinaryFPDistribution)
	itemProofSizeMin := common.BinaryItemProofSizeMin
	itemProofSizeMax := common.BinaryItemProofSizeMax
	common.BinaryStatsMu.Unlock()

	return silentReadStatsSnapshot{
		totalReads:                 atomic.LoadInt64(&common.TotalReads),
		totalAccountReads:          atomic.LoadInt64(&common.TotalAccountReads),
		cacheAccountHit:            atomic.LoadInt64(&common.CacheAccountHit),
		cacheAccountMissNotExists:  atomic.LoadInt64(&common.CacheAccountMissNotExists),
		cacheAccountMissExists:     atomic.LoadInt64(&common.CacheAccountMissExists),
		binaryHitCount:             atomic.LoadInt64(&common.BinaryHitCount),
		binaryMissNonExistentCount: atomic.LoadInt64(&common.BinaryMissNonExistentCount),
		binaryMissExistentCount:    atomic.LoadInt64(&common.BinaryMissExistentCount),
		binaryCycleFPCount:         atomic.LoadInt64(&common.BinaryCycleFPCount),
		binaryMaxFPInSingleBlock:   atomic.LoadInt64(&common.BinaryMaxFPInSingleBlock),
		binaryTrieFPInBlock:        atomic.LoadInt64(&common.BinaryTrieFPInBlock),
		binaryProofVerifTime:       atomic.LoadInt64(&common.BinaryProofVerifTime),
		binaryProofVerifTimeMax:    atomic.LoadInt64(&common.BinaryProofVerifTimeMax),
		binaryProofGenTime:         atomic.LoadInt64(&common.BinaryProofGenTime),
		binaryProofGenTimeMax:      atomic.LoadInt64(&common.BinaryProofGenTimeMax),
		binaryBlockProofSize:       atomic.LoadInt64(&common.BinaryBlockProofSize),
		binaryBlockProofSizeMax:    atomic.LoadInt64(&common.BinaryBlockProofSizeMax),
		binaryTotalProofSize:       atomic.LoadInt64(&common.BinaryTotalProofSize),
		binaryItemProofSizeMin:     itemProofSizeMin,
		binaryItemProofSizeMax:     itemProofSizeMax,
		binaryItemProofSizesLen:    itemProofSizesLen,
		binaryFPDistributionLen:    fpDistributionLen,
	}
}

func (s silentReadStatsSnapshot) restore() {
	atomic.StoreInt64(&common.TotalReads, s.totalReads)
	atomic.StoreInt64(&common.TotalAccountReads, s.totalAccountReads)
	atomic.StoreInt64(&common.CacheAccountHit, s.cacheAccountHit)
	atomic.StoreInt64(&common.CacheAccountMissNotExists, s.cacheAccountMissNotExists)
	atomic.StoreInt64(&common.CacheAccountMissExists, s.cacheAccountMissExists)
	atomic.StoreInt64(&common.BinaryHitCount, s.binaryHitCount)
	atomic.StoreInt64(&common.BinaryMissNonExistentCount, s.binaryMissNonExistentCount)
	atomic.StoreInt64(&common.BinaryMissExistentCount, s.binaryMissExistentCount)
	atomic.StoreInt64(&common.BinaryCycleFPCount, s.binaryCycleFPCount)
	atomic.StoreInt64(&common.BinaryMaxFPInSingleBlock, s.binaryMaxFPInSingleBlock)
	atomic.StoreInt64(&common.BinaryTrieFPInBlock, s.binaryTrieFPInBlock)
	atomic.StoreInt64(&common.BinaryProofVerifTime, s.binaryProofVerifTime)
	atomic.StoreInt64(&common.BinaryProofVerifTimeMax, s.binaryProofVerifTimeMax)
	atomic.StoreInt64(&common.BinaryProofGenTime, s.binaryProofGenTime)
	atomic.StoreInt64(&common.BinaryProofGenTimeMax, s.binaryProofGenTimeMax)
	atomic.StoreInt64(&common.BinaryBlockProofSize, s.binaryBlockProofSize)
	atomic.StoreInt64(&common.BinaryBlockProofSizeMax, s.binaryBlockProofSizeMax)
	atomic.StoreInt64(&common.BinaryTotalProofSize, s.binaryTotalProofSize)

	common.BinaryStatsMu.Lock()
	if len(common.BinaryItemProofSizes) > s.binaryItemProofSizesLen {
		common.BinaryItemProofSizes = common.BinaryItemProofSizes[:s.binaryItemProofSizesLen]
	}
	if len(common.BinaryFPDistribution) > s.binaryFPDistributionLen {
		common.BinaryFPDistribution = common.BinaryFPDistribution[:s.binaryFPDistributionLen]
	}
	common.BinaryItemProofSizeMin = s.binaryItemProofSizeMin
	common.BinaryItemProofSizeMax = s.binaryItemProofSizeMax
	common.BinaryStatsMu.Unlock()
}

// AddBalanceSilent performs a balance change on a StateDB without affecting the accumulated reading statistics.
func AddBalanceSilent(sdb *state.StateDB, addr common.Address, amount *uint256.Int) {
	if amount == nil || amount.IsZero() {
		return
	}

	stats := captureSilentReadStats()
	sdb.AddBalance(addr, amount, tracing.BalanceChangeUnspecified)
	stats.restore()
}

// SetCodeSilent performs a code update on a StateDB without affecting the accumulated reading statistics.
func SetCodeSilent(sdb *state.StateDB, addr common.Address, code []byte) {
	if len(code) == 0 {
		return
	}

	stats := captureSilentReadStats()
	sdb.SetCode(addr, code)
	stats.restore()
}

type txIndex struct {
	blockNum uint64
	offset   int64
}

// TransactionStreamer provides a streaming interface to read transactions from a CSV file block-by-block.
type TransactionStreamer struct {
	file    *os.File
	indices []txIndex
	curr    int // Index into indices slice
}

// NewTransactionStreamer opens a CSV file and initializes an index for memory-efficient streaming.
func NewTransactionStreamer(filePath string) (*TransactionStreamer, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	var indices []txIndex

	// Fast indexing pass using bufio.Reader
	bufr := bufio.NewReader(f)

	// Skip header line
	_, _, err = bufr.ReadLine()
	if err != nil {
		f.Close()
		return nil, err
	}

	offset, _ := f.Seek(0, os.SEEK_CUR)
	// Adjust offset based on what's left in the buffer
	offset -= int64(bufr.Buffered())

	for {
		lineOffset := offset
		line, err := bufr.ReadBytes('\n')
		if err != nil && len(line) == 0 {
			break
		}
		offset += int64(len(line))

		sline := string(line)
		if len(sline) < 10 || strings.HasPrefix(sline, "hash") {
			continue
		}

		// Fast block number extraction (block number is the 4th column, index 3)
		parts := strings.SplitN(sline, ",", 5)
		if len(parts) < 4 {
			continue
		}
		blockNum, _ := strconv.ParseUint(parts[3], 10, 64)
		indices = append(indices, txIndex{blockNum: blockNum, offset: lineOffset})
	}

	// Sort indices by block number
	sort.Slice(indices, func(i, j int) bool {
		if indices[i].blockNum == indices[j].blockNum {
			return indices[i].offset < indices[j].offset
		}
		return indices[i].blockNum < indices[j].blockNum
	})

	return &TransactionStreamer{
		file:    f,
		indices: indices,
		curr:    0,
	}, nil
}

// PeekBlockNum returns the block number of the next transaction record without consuming it.
func (s *TransactionStreamer) PeekBlockNum() (uint64, bool) {
	if s.curr >= len(s.indices) {
		return 0, false
	}
	return s.indices[s.curr].blockNum, true
}

// PopBlock consumes and returns all transaction messages for the specified block.
func (s *TransactionStreamer) PopBlock(targetBlock uint64) ([]*core.Message, bool) {
	if s.curr >= len(s.indices) || s.indices[s.curr].blockNum != targetBlock {
		return nil, false
	}

	var msgs []*core.Message
	// Use a shared buffer for reading lines to reduce allocations
	bufr := bufio.NewReader(s.file)

	for s.curr < len(s.indices) && s.indices[s.curr].blockNum == targetBlock {
		_, err := s.file.Seek(s.indices[s.curr].offset, os.SEEK_SET)
		if err != nil {
			s.curr++
			continue
		}
		bufr.Reset(s.file)
		line, _, err := bufr.ReadLine()
		if err == nil {
			// Fast parse the line
			reader := csv.NewReader(strings.NewReader(string(line)))
			record, err := reader.Read()
			if err == nil {
				msg, err := ParseCSVRecordToMessage(record)
				if err == nil {
					msgs = append(msgs, msg)
				}
			}
		}
		s.curr++
	}

	return msgs, len(msgs) > 0
}

// Close closes the underlying file handle.
func (s *TransactionStreamer) Close() {
	if s.file != nil {
		s.file.Close()
	}
}

// ParseCSVRecordToMessage parses a single CSV record into a core.Message.
func ParseCSVRecordToMessage(record []string) (*core.Message, error) {
	if len(record) < 10 {
		return nil, fmt.Errorf("invalid record length")
	}

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

	return &core.Message{
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
		SkipFromEOACheck: true,
	}, nil
}

// getDirSize returns the total size of a directory in bytes.
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

	/*
		if !common.DebugFlag {
			return
		}
	*/
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

// 存储区块高度到打包人的映射
var compareBlockMiners = make(map[uint64]common.Address)

// 从对应的区块文件中加载时间戳
func compareLoadBlockTimestampsFromFile(dataDir string, fileIndex string) error {
	// 清空旧数据，避免跨文件累积导致内存过高
	compareBlockTimestamps = make(map[uint64]uint64)
	compareBlockMiners = make(map[uint64]common.Address)

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

	// 查找number、timestamp和miner字段的索引
	var numberIdx, timestampIdx, minerIdx int = -1, -1, -1
	for i, header := range headers {
		switch strings.ToLower(header) {
		case "number":
			numberIdx = i
		case "timestamp":
			timestampIdx = i
		case "miner", "author", "coinbase":
			minerIdx = i
		}
	}

	if numberIdx == -1 {
		return fmt.Errorf("区块CSV文件 %s 缺少number字段", blockFile)
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
		if timestampIdx != -1 {
			timestamp, err := strconv.ParseUint(record[timestampIdx], 10, 64)
			if err == nil {
				compareBlockTimestamps[num] = timestamp
			}
		}

		// 解析打包人
		if minerIdx != -1 && record[minerIdx] != "" && record[minerIdx] != "null" {
			compareBlockMiners[num] = common.HexToAddress(record[minerIdx])
		}
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

	value := db.StateDB.GetState(addr, key)
	return value
}

// SetState 重写SetState方法，增加计数
func (db *CompareCountingStateDB) SetState(addr common.Address, key, value common.Hash) common.Hash {
	// 记录对存储槽位的写入
	stateKey := addr.Hex() + ":" + key.Hex()
	db.counter.RecordStateWrite(stateKey)

	// 同时记录对账户地址的写入
	addrStr := addr.Hex()
	db.counter.RecordStateWrite(addrStr)
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
