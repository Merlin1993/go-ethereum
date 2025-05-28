// Copyright 2023 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// 性能优化说明: 此文件已进行性能优化，不再保存每个区块的统计数据，只保存并输出10万区块的汇总统计。
// 这显著减少了内存使用和计算开销，特别是在长时间运行的节点上。

package core

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/cacheTrie"
)

// 常量定义
const (
	// 统计窗口大小
	StatsWindow = 100000 // 10万区块

	// CSV文件名前缀
	CacheTriePrefix  = "cachetrie_stats"
	TriePrefix       = "trie_stats"
	VerkleTriePrefix = "verkletrie_stats"

	// 单位转换
	MBSize = 1024.0 * 1024.0 // 转换为MB的除数
)

// TrieType 表示状态树的类型
type TrieType int

const (
	StandardTrie TrieType = iota
	CacheTrie
	VerkleTrie
)

// TrieStatsAggregator 状态树统计数据聚合器
type TrieStatsAggregator struct {
	mu           sync.Mutex // 保护内部状态的互斥锁
	OutputDir    string     // 输出目录
	Window       uint64     // 统计窗口(10w区块)
	LastOutput   uint64     // 上次输出统计的区块号
	TrieType     TrieType   // 树类型
	LastDataSize int64      // 上次记录的数据大小
	DataPath     string     // 数据路径(用于计算数据大小变化)

	// 累计统计数据
	WindowStats struct {
		// 第一类统计：需要累加的数据
		TotalWrittenStates  int64         // 窗口内写入状态总数
		TotalReadStates     int64         // 窗口内读取状态总数
		TotalExecTime       time.Duration // 窗口内交易执行总时间
		TotalRootGenTime    time.Duration // 窗口内根生成总时间
		TotalMemorySize     float64       // 窗口内内存大小总和
		TotalCacheSize      int64         // 窗口内缓存大小总和
		TotalCacheThreshold int64         // 窗口内缓存阈值总和
		MaxMemorySize       float64       // 最大内存使用量
		MaxCacheSize        int           // 最大缓存大小
		TotalTxCount        int64         // 总交易数
		SampleCount         int           // 窗口内样本数量
		CacheSampleCount    int           // 窗口内缓存样本数量

		// 区块范围记录
		StartBlock uint64 // 窗口起始区块
		EndBlock   uint64 // 窗口结束区块
	}

	// CacheTrie实例的引用，用于在输出时获取命中率和清理统计
	CacheTrieRef *cacheTrie.CacheTrie
}

// NewTrieStatsAggregator 创建一个新的状态树统计聚合器
func NewTrieStatsAggregator(outputDir string, dataPath string, trieType TrieType) *TrieStatsAggregator {
	// 确保输出目录存在
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		os.MkdirAll(outputDir, 0755)
	}

	// 获取初始数据大小
	var initialSize int64
	if dataPath != "" {
		initialSize, _ = CalculateDirSize(dataPath)
	}

	return &TrieStatsAggregator{
		OutputDir:    outputDir,
		Window:       StatsWindow,
		LastOutput:   0,
		TrieType:     trieType,
		LastDataSize: initialSize,
		DataPath:     dataPath,
		CacheTrieRef: nil,
	}
}

// AddBlockStats 添加一个区块的统计数据
func (s *TrieStatsAggregator) AddBlockStats(blockNum uint64, writtenStates, readStates, txCount int, txExecTime, rootGenTime time.Duration, memorySizeMB float64, cacheSize, cacheThreshold int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 特殊处理：如果这是第一个区块且恰好是10万的整数倍，我们不希望统计它
	// 只记录LastOutput，等待下一个区块开始统计
	if s.LastOutput == 0 && s.WindowStats.SampleCount == 0 && blockNum%s.Window == 0 {
		s.LastOutput = blockNum
		return
	}

	// 检查是否跳过了10万的整数倍高度
	// 例如：上次统计是在95000，当前区块是105000，中间跳过了100000这个整数倍高度
	if blockNum >= (s.LastOutput+s.Window)/s.Window*s.Window &&
		(blockNum/s.Window) > (s.LastOutput/s.Window) && blockNum%s.Window != 0 {
		// 已跳过了至少一个10万整数倍的高度，先输出上一个窗口的统计
		if s.WindowStats.SampleCount > 0 {
			s.OutputStats()
		}

		// 将LastOutput设为最近一个被跳过的10万整数倍
		// 计算当前区块所在的10万整数倍的前一个10万整数倍
		s.LastOutput = (blockNum / s.Window) * s.Window

		// 重置窗口统计
		s.resetWindowStats()
	}

	// 记录区块范围
	if s.WindowStats.StartBlock == 0 {
		s.WindowStats.StartBlock = blockNum
	}
	s.WindowStats.EndBlock = blockNum

	// 更新第一类统计：需要累加的数据
	s.WindowStats.TotalWrittenStates += int64(writtenStates)
	s.WindowStats.TotalReadStates += int64(readStates)
	s.WindowStats.TotalExecTime += txExecTime
	s.WindowStats.TotalRootGenTime += rootGenTime
	s.WindowStats.TotalTxCount += int64(txCount)
	s.WindowStats.SampleCount++

	// 如果是CacheTrie，更新缓存统计
	if s.TrieType == CacheTrie {
		s.WindowStats.TotalMemorySize += memorySizeMB
		s.WindowStats.TotalCacheSize += int64(cacheSize)
		s.WindowStats.TotalCacheThreshold += int64(cacheThreshold)
		s.WindowStats.CacheSampleCount++

		// 更新最大值
		if memorySizeMB > s.WindowStats.MaxMemorySize {
			s.WindowStats.MaxMemorySize = memorySizeMB
		}

		if cacheSize > s.WindowStats.MaxCacheSize {
			s.WindowStats.MaxCacheSize = cacheSize
		}
	}

	// 检查当前区块是否是10万的整数倍
	if blockNum >= s.Window && blockNum%s.Window == 0 {
		// 输出统计
		s.OutputStats()
		s.LastOutput = blockNum

		// 重置窗口统计
		s.resetWindowStats()
	}
}

// OutputStats 输出窗口(10万区块)统计数据
func (s *TrieStatsAggregator) OutputStats() {
	// 获取第三类统计：数据大小变化
	var dataSizeDelta int64
	if s.DataPath != "" {
		currentSize := s.GetCurrentDataSize()
		dataSizeDelta = currentSize - s.LastDataSize
		s.LastDataSize = currentSize
	}

	// 获取第二类统计：CacheTrie的命中率和清理统计
	var getHitRate, updateHitRate float64
	var cleanupCount int
	var totalCleanupTime, maxCleanupTime time.Duration
	var getHits, getMisses, updateHits, updateMisses uint64

	if s.TrieType == CacheTrie && s.CacheTrieRef != nil {
		// 获取命中率
		_, getHits, getMisses, getHitRate, _, updateHits, updateMisses, updateHitRate = s.CacheTrieRef.GetHitRate()

		// 获取清理统计
		cleanupCount = s.CacheTrieRef.GetCleanupCount()
		totalCleanupTime, maxCleanupTime = s.CacheTrieRef.GetCleanupTimes()

		// 重置统计数据
		s.CacheTrieRef.ResetStats()
	}

	// 打印统计信息到控制台
	s.printWindowStats(dataSizeDelta, getHitRate, updateHitRate, getHits, getMisses, updateHits, updateMisses, cleanupCount, totalCleanupTime, maxCleanupTime)

	// 输出CSV文件
	s.outputCSV(dataSizeDelta, getHitRate, updateHitRate, getHits, getMisses, updateHits, updateMisses, cleanupCount, totalCleanupTime, maxCleanupTime)
}

// GetCurrentDataSize 获取当前数据目录大小
func (s *TrieStatsAggregator) GetCurrentDataSize() int64 {
	if s.DataPath == "" {
		return 0
	}

	size, err := CalculateDirSize(s.DataPath)
	if err != nil {
		fmt.Printf("计算目录大小出错: %v\n", err)
		return 0
	}
	return size
}

// 内部辅助函数
func getTrieTypeName(trieType TrieType) string {
	switch trieType {
	case StandardTrie:
		return "Trie"
	case CacheTrie:
		return "CacheTrie"
	case VerkleTrie:
		return "VerkleTrie"
	default:
		return "Unknown"
	}
}

// CreateTrieStatsRecorder 创建一个状态树统计记录器
func CreateTrieStatsRecorder(outputDir, dataPath string, trieType TrieType) *TrieStatsAggregator {
	return NewTrieStatsAggregator(outputDir, dataPath, trieType)
}

// RecordTrieStats 记录标准Trie的状态树统计数据
func RecordTrieStats(recorder *TrieStatsAggregator, blockNum uint64, writtenStates, readStates, txCount int,
	txExecTime, rootGenTime time.Duration) {

	// 确保输出目录存在
	if _, err := os.Stat(recorder.OutputDir); os.IsNotExist(err) {
		os.MkdirAll(recorder.OutputDir, 0755)
	}

	// 添加基本统计数据
	recorder.AddBlockStats(blockNum, writtenStates, readStates, txCount, txExecTime, rootGenTime, 0, 0, 0)
}

// RecordCacheTrieStats 记录 CacheTrie 的状态树统计数据
func RecordCacheTrieStats(recorder *TrieStatsAggregator, blockNum uint64, writtenStates, readStates, txCount int,
	txExecTime, rootGenTime time.Duration, cacheTrie *cacheTrie.CacheTrie) {

	// 确保输出目录存在
	if _, err := os.Stat(recorder.OutputDir); os.IsNotExist(err) {
		os.MkdirAll(recorder.OutputDir, 0755)
	}

	// 保存CacheTrie实例的引用，用于在输出时获取命中率和清理统计
	recorder.mu.Lock()
	recorder.CacheTrieRef = cacheTrie
	recorder.mu.Unlock()

	// 获取当前的内存大小和缓存大小
	var memorySizeMB float64
	var cacheSize, cacheThreshold int

	if cacheTrie != nil {
		memorySize := cacheTrie.GetMemorySize()
		memorySizeMB = float64(memorySize) / MBSize
		cacheSize = cacheTrie.GetSize()
		cacheThreshold = cacheTrie.GetHRW().GetThreshold()
	}

	// 添加统计数据
	recorder.AddBlockStats(blockNum, writtenStates, readStates, txCount, txExecTime, rootGenTime, memorySizeMB, cacheSize, cacheThreshold)
}

// RecordVerkleTrieStats 记录 VerkleTrie 的状态树统计数据
func RecordVerkleTrieStats(recorder *TrieStatsAggregator, blockNum uint64, writtenStates, readStates, txCount int,
	txExecTime, rootGenTime time.Duration) {

	// 确保输出目录存在
	if _, err := os.Stat(recorder.OutputDir); os.IsNotExist(err) {
		os.MkdirAll(recorder.OutputDir, 0755)
	}

	// 添加基本统计数据
	recorder.AddBlockStats(blockNum, writtenStates, readStates, txCount, txExecTime, rootGenTime, 0, 0, 0)
}

// resetWindowStats 重置窗口统计数据
func (s *TrieStatsAggregator) resetWindowStats() {
	s.WindowStats = struct {
		// 第一类统计：需要累加的数据
		TotalWrittenStates  int64
		TotalReadStates     int64
		TotalExecTime       time.Duration
		TotalRootGenTime    time.Duration
		TotalMemorySize     float64
		TotalCacheSize      int64
		TotalCacheThreshold int64
		MaxMemorySize       float64
		MaxCacheSize        int
		TotalTxCount        int64
		SampleCount         int
		CacheSampleCount    int

		// 区块范围记录
		StartBlock uint64
		EndBlock   uint64
	}{}
}

// printWindowStats 打印窗口统计信息
func (s *TrieStatsAggregator) printWindowStats(dataSizeDelta int64, getHitRate, updateHitRate float64,
	getHits, getMisses, updateHits, updateMisses uint64,
	cleanupCount int, totalCleanupTime, maxCleanupTime time.Duration) {

	var typeStr string
	switch s.TrieType {
	case StandardTrie:
		typeStr = "Standard Trie"
	case CacheTrie:
		typeStr = "CacheTrie"
	case VerkleTrie:
		typeStr = "VerkleTrie"
	}

	// 计算平均时间（微秒）
	totalExecTimeMicros := s.WindowStats.TotalExecTime.Microseconds()
	totalRootGenTimeMicros := s.WindowStats.TotalRootGenTime.Microseconds()
	avgExecTimeMicros := totalExecTimeMicros
	avgRootGenTimeMicros := totalRootGenTimeMicros
	if s.WindowStats.SampleCount > 0 {
		avgExecTimeMicros = totalExecTimeMicros / int64(s.WindowStats.SampleCount)
		avgRootGenTimeMicros = totalRootGenTimeMicros / int64(s.WindowStats.SampleCount)
	}

	// 计算窗口编号
	windowNumber := s.WindowStats.EndBlock / s.Window

	fmt.Printf("\n===== [%s] 窗口 #%d 统计 (区块范围: %d - %d) =====\n",
		typeStr, windowNumber, s.WindowStats.StartBlock, s.WindowStats.EndBlock)

	fmt.Printf("总写入状态数: %d, 总读取状态数: %d\n",
		s.WindowStats.TotalWrittenStates, s.WindowStats.TotalReadStates)

	fmt.Printf("总交易数: %d\n", s.WindowStats.TotalTxCount)

	fmt.Printf("交易执行总时间: %d 微秒, 平均每区块: %d 微秒\n",
		totalExecTimeMicros, avgExecTimeMicros)
	fmt.Printf("根生成总时间: %d 微秒, 平均每区块: %d 微秒\n",
		totalRootGenTimeMicros, avgRootGenTimeMicros)

	// CacheTrie特有统计
	if s.TrieType == CacheTrie && s.WindowStats.CacheSampleCount > 0 {
		// 计算平均值
		avgMemorySize := s.WindowStats.TotalMemorySize / float64(s.WindowStats.CacheSampleCount)
		avgCacheSize := float64(s.WindowStats.TotalCacheSize) / float64(s.WindowStats.CacheSampleCount)
		avgThreshold := float64(s.WindowStats.TotalCacheThreshold) / float64(s.WindowStats.CacheSampleCount)

		fmt.Printf("\n----- CacheTrie特有统计 -----\n")
		fmt.Printf("Get命中率: %.2f%% (命中: %d, 未命中: %d, 总数: %d)\n",
			getHitRate*100, getHits, getMisses, getHits+getMisses)
		fmt.Printf("Update命中率: %.2f%% (命中: %d, 未命中: %d, 总数: %d)\n",
			updateHitRate*100, updateHits, updateMisses, updateHits+updateMisses)

		fmt.Printf("平均内存使用: %.2f MB, 最大内存使用: %.2f MB\n", avgMemorySize, s.WindowStats.MaxMemorySize)
		fmt.Printf("平均缓存大小: %.2f, 最大缓存大小: %d, 平均阈值: %.2f\n",
			avgCacheSize, s.WindowStats.MaxCacheSize, avgThreshold)

		// 显示总清理时间和总清理次数
		fmt.Printf("总清理时间: %d 微秒, 总清理次数: %d, 最大单次清理时间: %d 微秒\n",
			totalCleanupTime.Microseconds(), cleanupCount, maxCleanupTime.Microseconds())
	}

	// 数据大小变化
	if s.DataPath != "" {
		dataSizeDeltaMB := float64(dataSizeDelta) / MBSize

		fmt.Printf("\n----- 数据大小统计 -----\n")
		fmt.Printf("数据大小总变化: %.2f MB\n", dataSizeDeltaMB)
	}

	fmt.Println("=======================================")
}

// outputCSV 输出CSV文件
func (s *TrieStatsAggregator) outputCSV(dataSizeDelta int64, getHitRate, updateHitRate float64,
	getHits, getMisses, updateHits, updateMisses uint64,
	cleanupCount int, totalCleanupTime, maxCleanupTime time.Duration) {

	var prefix string
	switch s.TrieType {
	case StandardTrie:
		prefix = TriePrefix
	case CacheTrie:
		prefix = CacheTriePrefix
	case VerkleTrie:
		prefix = VerkleTriePrefix
	}

	// 使用固定文件名
	filename := fmt.Sprintf("%s.csv", prefix)
	filePath := filepath.Join(s.OutputDir, filename)

	// 确定当前数据行号 - 第几个窗口
	windowNumber := s.WindowStats.EndBlock / s.Window

	// 准备当前数据记录
	// 计算平均值
	var avgExecTime, avgRootGenTime int64
	if s.WindowStats.SampleCount > 0 {
		avgExecTime = s.WindowStats.TotalExecTime.Microseconds() / int64(s.WindowStats.SampleCount)
		avgRootGenTime = s.WindowStats.TotalRootGenTime.Microseconds() / int64(s.WindowStats.SampleCount)
	}

	// 数据大小变化（MB）
	dataSizeDeltaMB := float64(dataSizeDelta) / MBSize

	// 准备当前记录，第一个字段改为窗口编号
	currentRecord := []string{
		fmt.Sprintf("%d", windowNumber), // 第几个10万区块，从0开始
		strconv.FormatInt(s.WindowStats.TotalWrittenStates, 10),
		strconv.FormatInt(s.WindowStats.TotalReadStates, 10),
		strconv.FormatInt(s.WindowStats.TotalTxCount, 10),
		strconv.FormatInt(avgExecTime, 10),
		strconv.FormatInt(avgRootGenTime, 10),
		strconv.FormatFloat(dataSizeDeltaMB, 'f', 2, 64),
	}

	// 如果是CacheTrie，添加额外的统计
	if s.TrieType == CacheTrie && s.WindowStats.CacheSampleCount > 0 {
		// 计算平均值
		avgMemorySize := s.WindowStats.TotalMemorySize / float64(s.WindowStats.CacheSampleCount)
		avgCacheSize := float64(s.WindowStats.TotalCacheSize) / float64(s.WindowStats.CacheSampleCount)
		avgThreshold := float64(s.WindowStats.TotalCacheThreshold) / float64(s.WindowStats.CacheSampleCount)

		currentRecord = append(currentRecord,
			strconv.FormatFloat(avgMemorySize, 'f', 2, 64),
			strconv.FormatFloat(s.WindowStats.MaxMemorySize, 'f', 2, 64),
			strconv.FormatFloat(avgCacheSize, 'f', 2, 64),
			strconv.Itoa(s.WindowStats.MaxCacheSize),
			strconv.FormatFloat(avgThreshold, 'f', 2, 64),
			strconv.FormatInt(totalCleanupTime.Microseconds(), 10),
			strconv.Itoa(cleanupCount),
			strconv.FormatInt(maxCleanupTime.Microseconds(), 10),
			strconv.FormatFloat(getHitRate*100, 'f', 2, 64),
			strconv.FormatFloat(updateHitRate*100, 'f', 2, 64))
	}

	// 准备头部记录，更新第一个字段名称
	var headers []string
	headers = append(headers,
		"WindowNumber", // 第几个10万区块
		"TotalWrittenStates",
		"TotalReadStates",
		"TotalTransactionCount",
		"AvgTransactionExecTime(us)",
		"AvgRootGenTime(us)",
		"DataSizeDelta(MB)")

	// 如果是CacheTrie，添加额外的列
	if s.TrieType == CacheTrie {
		headers = append(headers,
			"AvgMemorySize(MB)",
			"MaxMemorySize(MB)",
			"AvgCacheSize",
			"MaxCacheSize",
			"AvgCacheThreshold",
			"TotalCleanupTime(us)",
			"TotalCleanupCount",
			"MaxCleanupTime(us)",
			"GetHitRate(%)",
			"UpdateHitRate(%)")
	}

	// 检查文件是否存在
	fileExists := false
	if _, err := os.Stat(filePath); err == nil {
		fileExists = true
	}

	if !fileExists {
		// 文件不存在，创建新文件
		file, err := os.Create(filePath)
		if err != nil {
			fmt.Printf("创建CSV文件失败: %v\n", err)
			return
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()

		// 写入头部
		writer.Write(headers)
		// 写入当前记录
		writer.Write(currentRecord)
		fmt.Printf("已创建CSV文件: %s\n", filePath)
	} else {
		// 文件已存在，读取现有记录
		allRecords, err := readCSVRecords(filePath)
		if err != nil {
			fmt.Printf("读取CSV文件失败: %v\n", err)
			return
		}

		// 确保records数组有足够的长度容纳当前窗口
		targetLength := int(windowNumber) + 2 // +1是因为窗口编号从0开始，+1是因为第一行是表头
		if len(allRecords) < targetLength {
			// 扩展数组长度
			oldLength := len(allRecords)
			for i := oldLength; i < targetLength; i++ {
				if i == 0 {
					// 如果没有表头，添加表头
					allRecords = append(allRecords, headers)
				} else {
					// 添加空记录占位
					allRecords = append(allRecords, make([]string, len(headers)))
				}
			}
		}

		// 更新或添加当前窗口的记录
		allRecords[windowNumber+1] = currentRecord // +1是因为第一行是表头

		// 重写文件
		file, err := os.Create(filePath)
		if err != nil {
			fmt.Printf("创建CSV文件失败: %v\n", err)
			return
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()

		// 写入所有非空记录
		for i, record := range allRecords {
			// 跳过空记录
			if i > 0 && len(record[0]) == 0 {
				continue
			}
			writer.Write(record)
		}

		fmt.Printf("已更新CSV文件: %s (窗口 #%d)\n", filePath, windowNumber)
	}
}

// readCSVRecords 读取CSV文件中的所有记录
func readCSVRecords(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	return reader.ReadAll()
}
