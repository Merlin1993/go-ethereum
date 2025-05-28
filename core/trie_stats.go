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

package core

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
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

// TrieBlockStats 记录每个区块的状态树统计数据
type TrieBlockStats struct {
	BlockNum            uint64        // 区块号
	WrittenStates       int           // 写入的状态总数量
	ReadStates          int           // 读取的状态总数量
	TransactionCount    int           // 交易量
	TransactionExecTime time.Duration // 交易运行花费时间
	RootGenTime         time.Duration // 树根生成花费时间
	DataSizeDelta       int64         // 树所在文件夹的数据量变化
	TrieType            TrieType      // 树类型

	// CacheTrie特有数据
	CacheHitRate   float64       // 缓存命中率
	MemorySizeMB   float64       // 内存大小(MB)
	CacheSize      int           // CacheTrie的size
	CacheThreshold int           // CacheTrie的阈值
	CleanupTime    time.Duration // 清理时间
	CleanupCount   int           // 清理次数

	// 新增字段: 详细的命中率数据
	GetHitCount     uint64  // Get操作命中次数
	GetMissCount    uint64  // Get操作未命中次数
	GetHitRate      float64 // Get操作命中率
	UpdateHitCount  uint64  // Update操作命中次数
	UpdateMissCount uint64  // Update操作未命中次数
	UpdateHitRate   float64 // Update操作命中率
}

// TrieStatsAggregator 状态树统计数据聚合器
type TrieStatsAggregator struct {
	mu           sync.Mutex       // 保护内部状态的互斥锁
	Stats        []TrieBlockStats // 所有区块的统计数据
	OutputDir    string           // 输出目录
	Window       uint64           // 统计窗口(10w区块)
	LastOutput   uint64           // 上次输出统计的区块号
	TrieType     TrieType         // 树类型
	LastDataSize int64            // 上次记录的数据大小
	DataPath     string           // 数据路径(用于计算数据大小变化)

	// 新增字段用于跟踪命中率重置
	LastHitRateReset        uint64       // 上次重置命中率的区块号
	HitRateResetWindow      uint64       // 命中率重置窗口(默认与Window相同)
	AccumulatedHitRateStats HitRateStats // 累积的命中率统计，在重置窗口后输出

	// 累计统计数据
	WindowStats struct {
		TotalWrittenStates  int64         // 窗口内写入状态总数
		TotalReadStates     int64         // 窗口内读取状态总数
		TotalExecTime       time.Duration // 窗口内交易执行总时间
		TotalRootGenTime    time.Duration // 窗口内根生成总时间
		TotalCacheHitRate   float64       // 窗口内缓存命中率总和
		TotalMemorySize     float64       // 窗口内内存大小总和
		TotalCacheSize      int64         // 窗口内缓存大小总和
		TotalCacheThreshold int64         // 窗口内缓存阈值总和
		TotalCleanupTime    time.Duration // 窗口内清理时间总和
		TotalCleanupCount   int           // 窗口内清理次数总和
		SampleCount         int           // 窗口内样本数量
		CacheSampleCount    int           // 窗口内缓存样本数量

		// 命中率详细统计
		TotalGetHits      uint64  // Get命中总数
		TotalGetMisses    uint64  // Get未命中总数
		TotalUpdateHits   uint64  // Update命中总数
		TotalUpdateMisses uint64  // Update未命中总数
		MaxMemorySize     float64 // 最大内存使用量
	}
}

// HitRateStats 存储命中率统计数据
type HitRateStats struct {
	GetHits       uint64  // Get命中次数
	GetMisses     uint64  // Get未命中次数
	GetHitRate    float64 // Get命中率
	UpdateHits    uint64  // Update命中次数
	UpdateMisses  uint64  // Update未命中次数
	UpdateHitRate float64 // Update命中率
	StartBlock    uint64  // 起始区块号
	EndBlock      uint64  // 结束区块号
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

	// 查找最新的区块号，以支持断点续跑
	lastBlock := findLastOutputBlock(outputDir, trieType)

	return &TrieStatsAggregator{
		Stats:              make([]TrieBlockStats, 0),
		OutputDir:          outputDir,
		Window:             StatsWindow,
		LastOutput:         lastBlock,
		TrieType:           trieType,
		LastDataSize:       initialSize,
		DataPath:           dataPath,
		LastHitRateReset:   lastBlock,
		HitRateResetWindow: StatsWindow, // 默认与统计窗口相同
	}
}

// 查找最后一次输出的区块号，用于断点续跑
func findLastOutputBlock(outputDir string, trieType TrieType) uint64 {
	var prefix string
	switch trieType {
	case StandardTrie:
		prefix = TriePrefix
	case CacheTrie:
		prefix = CacheTriePrefix
	case VerkleTrie:
		prefix = VerkleTriePrefix
	}

	// 查找窗口CSV文件
	pattern := filepath.Join(outputDir, fmt.Sprintf("%s_*.csv", prefix))
	files, _ := filepath.Glob(pattern)

	var lastBlock uint64

	// 从文件名中提取最后的区块号
	if len(files) > 0 {
		// 解析文件名格式：prefix_startBlock_endBlock.csv
		for _, file := range files {
			base := filepath.Base(file)
			parts := strings.Split(base, "_")
			if len(parts) >= 3 {
				// 获取结束区块号
				endStr := strings.TrimSuffix(parts[len(parts)-1], ".csv")
				endBlock, err := strconv.ParseUint(endStr, 10, 64)
				if err == nil && endBlock > lastBlock {
					lastBlock = endBlock
				}
			}
		}
	}

	return lastBlock
}

// AddBlockStats 添加一个区块的统计数据
func (s *TrieStatsAggregator) AddBlockStats(stats TrieBlockStats) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 保存统计数据
	s.Stats = append(s.Stats, stats)

	// 更新窗口累计统计
	s.WindowStats.TotalWrittenStates += int64(stats.WrittenStates)
	s.WindowStats.TotalReadStates += int64(stats.ReadStates)
	s.WindowStats.TotalExecTime += stats.TransactionExecTime
	s.WindowStats.TotalRootGenTime += stats.RootGenTime
	s.WindowStats.SampleCount++

	// 如果是CacheTrie，更新缓存统计
	if stats.TrieType == CacheTrie {
		s.WindowStats.TotalCacheHitRate += stats.CacheHitRate
		s.WindowStats.TotalMemorySize += stats.MemorySizeMB
		s.WindowStats.TotalCacheSize += int64(stats.CacheSize)
		s.WindowStats.TotalCacheThreshold += int64(stats.CacheThreshold)
		s.WindowStats.TotalCleanupTime += stats.CleanupTime
		s.WindowStats.TotalCleanupCount += stats.CleanupCount
		s.WindowStats.CacheSampleCount++

		// 更新命中率详细统计
		s.WindowStats.TotalGetHits += stats.GetHitCount
		s.WindowStats.TotalGetMisses += stats.GetMissCount
		s.WindowStats.TotalUpdateHits += stats.UpdateHitCount
		s.WindowStats.TotalUpdateMisses += stats.UpdateMissCount

		// 更新最大内存使用量
		if stats.MemorySizeMB > s.WindowStats.MaxMemorySize {
			s.WindowStats.MaxMemorySize = stats.MemorySizeMB
		}
	}

	// 检查是否需要输出窗口统计
	if stats.BlockNum-s.LastOutput >= s.Window {
		s.OutputStats()
		s.LastOutput = stats.BlockNum

		// 重置窗口统计
		s.WindowStats = struct {
			TotalWrittenStates  int64
			TotalReadStates     int64
			TotalExecTime       time.Duration
			TotalRootGenTime    time.Duration
			TotalCacheHitRate   float64
			TotalMemorySize     float64
			TotalCacheSize      int64
			TotalCacheThreshold int64
			TotalCleanupTime    time.Duration
			TotalCleanupCount   int
			SampleCount         int
			CacheSampleCount    int

			// 新增字段：命中率详细统计
			TotalGetHits      uint64
			TotalGetMisses    uint64
			TotalUpdateHits   uint64
			TotalUpdateMisses uint64
			MaxMemorySize     float64
		}{}
	}
}

// OutputStats 输出窗口(10万区块)统计数据
func (s *TrieStatsAggregator) OutputStats() {
	if len(s.Stats) == 0 {
		return
	}

	// 打印统计信息到控制台
	s.printWindowStats()

	// 输出CSV文件
	startBlock := s.LastOutput
	endBlock := s.Stats[len(s.Stats)-1].BlockNum
	s.outputCSV(startBlock, endBlock)
}

// printWindowStats 打印窗口统计信息
func (s *TrieStatsAggregator) printWindowStats() {
	if len(s.Stats) == 0 {
		return
	}

	var typeStr string
	switch s.TrieType {
	case StandardTrie:
		typeStr = "Standard Trie"
	case CacheTrie:
		typeStr = "CacheTrie"
	case VerkleTrie:
		typeStr = "VerkleTrie"
	}

	startBlock := s.Stats[0].BlockNum
	endBlock := s.Stats[len(s.Stats)-1].BlockNum

	// 1. 改为统计10万区块内的总量，而不是平均值
	totalWrittenStates := s.WindowStats.TotalWrittenStates
	totalReadStates := s.WindowStats.TotalReadStates

	// 2. 计算交易总量
	var totalTxCount int64
	for _, stat := range s.Stats {
		totalTxCount += int64(stat.TransactionCount)
	}

	// 3. 转换为微秒单位
	totalExecTimeMicros := s.WindowStats.TotalExecTime.Microseconds()
	totalRootGenTimeMicros := s.WindowStats.TotalRootGenTime.Microseconds()
	avgExecTimeMicros := totalExecTimeMicros
	avgRootGenTimeMicros := totalRootGenTimeMicros
	if s.WindowStats.SampleCount > 0 {
		avgExecTimeMicros = totalExecTimeMicros / int64(s.WindowStats.SampleCount)
		avgRootGenTimeMicros = totalRootGenTimeMicros / int64(s.WindowStats.SampleCount)
	}

	fmt.Printf("\n===== [%s] 区块统计 (区块范围: %d - %d) =====\n",
		typeStr, startBlock, endBlock)

	fmt.Printf("总写入状态数: %d, 总读取状态数: %d\n",
		totalWrittenStates, totalReadStates)

	fmt.Printf("总交易数: %d\n", totalTxCount)

	fmt.Printf("交易执行总时间: %d 微秒, 平均每区块: %d 微秒\n",
		totalExecTimeMicros, avgExecTimeMicros)
	fmt.Printf("根生成总时间: %d 微秒, 平均每区块: %d 微秒\n",
		totalRootGenTimeMicros, avgRootGenTimeMicros)

	// 4. 分别显示update和get命中率，并给出具体数字
	if s.TrieType == CacheTrie && s.WindowStats.CacheSampleCount > 0 {
		// 使用累积的命中率统计，确保是完整的窗口数据
		var getHits, getMisses, updateHits, updateMisses uint64
		var getRate, updateRate float64

		// 使用最近一次重置周期的累积统计
		s.mu.Lock()
		hitRateStats := s.AccumulatedHitRateStats
		s.mu.Unlock()

		if hitRateStats.EndBlock > 0 {
			getHits = hitRateStats.GetHits
			getMisses = hitRateStats.GetMisses
			updateHits = hitRateStats.UpdateHits
			updateMisses = hitRateStats.UpdateMisses
			getRate = hitRateStats.GetHitRate
			updateRate = hitRateStats.UpdateHitRate

			fmt.Printf("\n----- CacheTrie命中率统计 (区块范围: %d - %d) -----\n",
				hitRateStats.StartBlock, hitRateStats.EndBlock)
		} else {
			// 如果没有累积的统计，则使用窗口统计数据
			getHits = s.WindowStats.TotalGetHits
			getMisses = s.WindowStats.TotalGetMisses
			updateHits = s.WindowStats.TotalUpdateHits
			updateMisses = s.WindowStats.TotalUpdateMisses

			getTotal := getHits + getMisses
			updateTotal := updateHits + updateMisses

			if getTotal > 0 {
				getRate = float64(getHits) / float64(getTotal) * 100
			}

			if updateTotal > 0 {
				updateRate = float64(updateHits) / float64(updateTotal) * 100
			}
		}

		// 检查阈值为什么是0
		avgThreshold := float64(s.WindowStats.TotalCacheThreshold) / float64(s.WindowStats.CacheSampleCount)
		thresholdComment := ""
		if avgThreshold <= 0 {
			thresholdComment = " (警告: 阈值为0，可能是HRW未正确初始化或GetThreshold未实现)"
		}

		fmt.Printf("\n----- CacheTrie特有统计 -----\n")
		fmt.Printf("Get命中率: %.2f%% (命中: %d, 未命中: %d, 总数: %d)\n",
			getRate, getHits, getMisses, getHits+getMisses)
		fmt.Printf("Update命中率: %.2f%% (命中: %d, 未命中: %d, 总数: %d)\n",
			updateRate, updateHits, updateMisses, updateHits+updateMisses)

		fmt.Printf("最大内存使用: %.2f MB\n", s.WindowStats.MaxMemorySize)
		fmt.Printf("平均缓存大小: %.2f, 平均阈值: %.2f%s\n",
			float64(s.WindowStats.TotalCacheSize)/float64(s.WindowStats.CacheSampleCount),
			avgThreshold, thresholdComment)

		// 显示总清理时间和总清理次数
		fmt.Printf("总清理时间: %d 微秒, 总清理次数: %d\n",
			s.WindowStats.TotalCleanupTime.Microseconds(), s.WindowStats.TotalCleanupCount)
	}

	// 计算数据大小变化
	if s.DataPath != "" {
		currentSize, _ := CalculateDirSize(s.DataPath)
		dataSizeDelta := currentSize - s.LastDataSize
		avgDataSizeDelta := float64(dataSizeDelta) / MBSize

		fmt.Printf("\n----- 数据大小统计 -----\n")
		fmt.Printf("数据大小变化: %.2f MB\n", avgDataSizeDelta)

		// 更新最新数据大小
		s.LastDataSize = currentSize
	}

	fmt.Println("=======================================")
}

// outputCSV 输出CSV文件
func (s *TrieStatsAggregator) outputCSV(startBlock, endBlock uint64) {
	if len(s.Stats) == 0 {
		return
	}

	var prefix string
	switch s.TrieType {
	case StandardTrie:
		prefix = TriePrefix
	case CacheTrie:
		prefix = CacheTriePrefix
	case VerkleTrie:
		prefix = VerkleTriePrefix
	}

	filename := fmt.Sprintf("%s_%d_%d.csv",
		prefix, startBlock, endBlock)

	filePath := filepath.Join(s.OutputDir, filename)

	file, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("创建CSV文件失败: %v\n", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入CSV头
	var headers []string
	headers = append(headers,
		"BlockNum",
		"WrittenStates",
		"ReadStates",
		"TransactionCount",
		"TransactionExecTime(us)", // 改为微秒单位
		"RootGenTime(us)",         // 改为微秒单位
		"DataSizeDelta(MB)")

	// 如果是CacheTrie，添加额外的列
	if s.TrieType == CacheTrie {
		headers = append(headers,
			"CacheHitRate(%)",
			"MemorySize(MB)",
			"CacheSize",
			"CacheThreshold",
			"CleanupTime(us)", // 改为微秒单位
			"CleanupCount",
			"GetHitCount",      // 新增
			"GetMissCount",     // 新增
			"GetHitRate(%)",    // 新增
			"UpdateHitCount",   // 新增
			"UpdateMissCount",  // 新增
			"UpdateHitRate(%)") // 新增
	}

	writer.Write(headers)

	// 写入数据
	for _, stat := range s.Stats {
		record := []string{
			strconv.FormatUint(stat.BlockNum, 10),
			strconv.Itoa(stat.WrittenStates),
			strconv.Itoa(stat.ReadStates),
			strconv.Itoa(stat.TransactionCount),
			strconv.FormatInt(stat.TransactionExecTime.Microseconds(), 10), // 改为微秒
			strconv.FormatInt(stat.RootGenTime.Microseconds(), 10),         // 改为微秒
			strconv.FormatFloat(float64(stat.DataSizeDelta)/MBSize, 'f', 2, 64),
		}

		// 如果是CacheTrie，添加额外的列
		if s.TrieType == CacheTrie {
			record = append(record,
				strconv.FormatFloat(stat.CacheHitRate, 'f', 2, 64),
				strconv.FormatFloat(stat.MemorySizeMB, 'f', 2, 64),
				strconv.Itoa(stat.CacheSize),
				strconv.Itoa(stat.CacheThreshold),
				strconv.FormatInt(stat.CleanupTime.Microseconds(), 10), // 改为微秒
				strconv.Itoa(stat.CleanupCount),
				strconv.FormatUint(stat.GetHitCount, 10),            // 新增
				strconv.FormatUint(stat.GetMissCount, 10),           // 新增
				strconv.FormatFloat(stat.GetHitRate, 'f', 2, 64),    // 新增
				strconv.FormatUint(stat.UpdateHitCount, 10),         // 新增
				strconv.FormatUint(stat.UpdateMissCount, 10),        // 新增
				strconv.FormatFloat(stat.UpdateHitRate, 'f', 2, 64), // 新增
			)
		}

		writer.Write(record)
	}

	fmt.Printf("已输出CSV文件: %s\n", filePath)

	// 清空统计数据，释放内存
	s.Stats = make([]TrieBlockStats, 0)
}

// GetCurrentDataSize 获取当前数据目录大小
func (s *TrieStatsAggregator) GetCurrentDataSize() int64 {
	if s.DataPath == "" {
		return 0
	}

	size, err := CalculateDirSize(s.DataPath)
	if err != nil {
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

// 以下是用于实际实现的支持函数

// CreateTrieStatsRecorder 创建一个状态树统计记录器
func CreateTrieStatsRecorder(outputDir, dataPath string, trieType TrieType) *TrieStatsAggregator {
	return NewTrieStatsAggregator(outputDir, dataPath, trieType)
}

// RecordTrieStats 记录状态树统计数据
func RecordTrieStats(recorder *TrieStatsAggregator, blockNum uint64, writtenStates, readStates, txCount int,
	txExecTime, rootGenTime time.Duration) {

	// 确保输出目录存在
	if _, err := os.Stat(recorder.OutputDir); os.IsNotExist(err) {
		os.MkdirAll(recorder.OutputDir, 0755)
	}

	// 获取当前数据大小
	currentSize := recorder.GetCurrentDataSize()
	dataSizeDelta := currentSize - recorder.LastDataSize

	// 创建基础统计数据
	stats := TrieBlockStats{
		BlockNum:            blockNum,
		WrittenStates:       writtenStates,
		ReadStates:          readStates,
		TransactionCount:    txCount,
		TransactionExecTime: txExecTime,
		RootGenTime:         rootGenTime,
		DataSizeDelta:       dataSizeDelta,
		TrieType:            recorder.TrieType,
	}

	// 添加统计数据
	recorder.AddBlockStats(stats)
}

// RecordCacheTrieStats 记录 CacheTrie 的状态树统计数据
func RecordCacheTrieStats(recorder *TrieStatsAggregator, blockNum uint64, writtenStates, readStates, txCount int,
	txExecTime, rootGenTime time.Duration, cacheTrie interface{}) {

	// 确保输出目录存在
	if _, err := os.Stat(recorder.OutputDir); os.IsNotExist(err) {
		os.MkdirAll(recorder.OutputDir, 0755)
	}

	// 获取当前数据大小
	currentSize := recorder.GetCurrentDataSize()
	dataSizeDelta := currentSize - recorder.LastDataSize

	// 创建基础统计数据
	stats := TrieBlockStats{
		BlockNum:            blockNum,
		WrittenStates:       writtenStates,
		ReadStates:          readStates,
		TransactionCount:    txCount,
		TransactionExecTime: txExecTime,
		RootGenTime:         rootGenTime,
		DataSizeDelta:       dataSizeDelta,
		TrieType:            CacheTrie,
	}

	// 如果提供了 CacheTrie 实例，获取额外的缓存统计信息
	if cacheTrie != nil {
		// 使用类型断言获取统计接口
		if ct, ok := cacheTrie.(interface {
			GetHitRate() (uint64, uint64, uint64, float64, uint64, uint64, uint64, float64)
			GetMemorySize() int64
			GetSize() int
			ResetStats()
		}); ok {
			// 检查是否需要获取命中率并重置
			needResetStats := (blockNum-recorder.LastHitRateReset >= recorder.HitRateResetWindow)

			// 获取当前的内存大小和缓存大小
			memorySize := ct.GetMemorySize()
			memorySizeMB := float64(memorySize) / MBSize
			stats.MemorySizeMB = memorySizeMB
			stats.CacheSize = ct.GetSize()

			// 如果达到了命中率重置窗口，获取并重置统计
			if needResetStats {
				// 获取详细命中率
				getHit, getMiss, _, getHitRate, updateHit, updateMiss, _, updateHitRate := ct.GetHitRate()

				// 更新累积的命中率统计
				recorder.mu.Lock()
				recorder.AccumulatedHitRateStats = HitRateStats{
					GetHits:       getHit,
					GetMisses:     getMiss,
					GetHitRate:    getHitRate * 100, // 转换为百分比
					UpdateHits:    updateHit,
					UpdateMisses:  updateMiss,
					UpdateHitRate: updateHitRate * 100, // 转换为百分比
					StartBlock:    recorder.LastHitRateReset,
					EndBlock:      blockNum,
				}

				// 设置详细的命中率数据
				stats.GetHitCount = getHit
				stats.GetMissCount = getMiss
				stats.GetHitRate = getHitRate * 100 // 转换为百分比
				stats.UpdateHitCount = updateHit
				stats.UpdateMissCount = updateMiss
				stats.UpdateHitRate = updateHitRate * 100 // 转换为百分比

				// 综合命中率 (原有逻辑保留)
				hitRate := (getHitRate + updateHitRate) * 50 // 两者各占50%
				stats.CacheHitRate = hitRate

				// 更新上次重置的区块号
				recorder.LastHitRateReset = blockNum
				recorder.mu.Unlock()

				// 重置命中率统计
				ct.ResetStats()

				fmt.Printf("在区块 %d 获取并重置CacheTrie命中率统计\n", blockNum)
			} else {
				// 如果不需要重置，使用累积的统计数据（如果有）
				recorder.mu.Lock()
				if recorder.AccumulatedHitRateStats.EndBlock > 0 {
					stats.GetHitCount = recorder.AccumulatedHitRateStats.GetHits
					stats.GetMissCount = recorder.AccumulatedHitRateStats.GetMisses
					stats.GetHitRate = recorder.AccumulatedHitRateStats.GetHitRate
					stats.UpdateHitCount = recorder.AccumulatedHitRateStats.UpdateHits
					stats.UpdateMissCount = recorder.AccumulatedHitRateStats.UpdateMisses
					stats.UpdateHitRate = recorder.AccumulatedHitRateStats.UpdateHitRate
					stats.CacheHitRate = (stats.GetHitRate + stats.UpdateHitRate) / 2
				}
				recorder.mu.Unlock()
			}
		}
	}

	// 添加统计数据
	recorder.AddBlockStats(stats)
}

// RecordVerkleTrieStats 记录 VerkleTrie 的状态树统计数据
func RecordVerkleTrieStats(recorder *TrieStatsAggregator, blockNum uint64, writtenStates, readStates, txCount int,
	txExecTime, rootGenTime time.Duration) {

	// 确保输出目录存在
	if _, err := os.Stat(recorder.OutputDir); os.IsNotExist(err) {
		os.MkdirAll(recorder.OutputDir, 0755)
	}

	// 获取当前数据大小
	currentSize := recorder.GetCurrentDataSize()
	dataSizeDelta := currentSize - recorder.LastDataSize

	// 创建基础统计数据
	stats := TrieBlockStats{
		BlockNum:            blockNum,
		WrittenStates:       writtenStates,
		ReadStates:          readStates,
		TransactionCount:    txCount,
		TransactionExecTime: txExecTime,
		RootGenTime:         rootGenTime,
		DataSizeDelta:       dataSizeDelta,
		TrieType:            VerkleTrie,
	}

	// 添加统计数据
	recorder.AddBlockStats(stats)
}

// SetCacheTrieExtraStats 设置CacheTrie的额外统计信息
func SetCacheTrieExtraStats(recorder *TrieStatsAggregator, threshold int, cleanupTime time.Duration, cleanupCount int) {
	if len(recorder.Stats) == 0 {
		return
	}

	// 获取最后一个统计数据并更新额外信息
	lastIdx := len(recorder.Stats) - 1
	if recorder.Stats[lastIdx].TrieType == CacheTrie {
		recorder.Stats[lastIdx].CacheThreshold = threshold
		recorder.Stats[lastIdx].CleanupTime = cleanupTime
		recorder.Stats[lastIdx].CleanupCount = cleanupCount
	}
}
