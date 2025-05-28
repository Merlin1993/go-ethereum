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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestTrieStatsAggregator 测试状态树统计聚合器
func TestTrieStatsAggregator(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "trie_stats_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建统计聚合器
	statsDir := filepath.Join(tempDir, "stats")
	dataDir := filepath.Join(tempDir, "data")

	// 创建数据目录和一些测试文件
	os.MkdirAll(dataDir, 0755)
	createDummyFiles(t, dataDir, 5, 1024) // 创建5个1KB的测试文件

	// 测试普通Trie的统计
	t.Run("StandardTrie", func(t *testing.T) {
		// 创建一个新的统计聚合器
		trieStats := NewTrieStatsAggregator(statsDir, dataDir, StandardTrie)

		// 记录一些测试数据
		addTestTrieStats(trieStats, 100000)

		// 检查统计信息是否正确 - 在输出之前检查
		if trieStats.WindowStats.SampleCount == 0 {
			t.Fatalf("无法添加统计数据")
		}

		// 手动触发输出
		trieStats.OutputStats()

		// 检查CSV文件是否创建
		files, err := filepath.Glob(filepath.Join(statsDir, "trie_stats_*.csv"))
		if err != nil || len(files) == 0 {
			t.Fatalf("未创建CSV文件，错误: %v", err)
		}
	})

	// 测试CacheTrie的统计
	t.Run("CacheTrie", func(t *testing.T) {
		// 创建一个新的统计聚合器
		cacheStats := NewTrieStatsAggregator(statsDir, dataDir, CacheTrie)

		// 记录一些测试数据，包括缓存统计
		addTestCacheTrieStats(cacheStats, 100000)

		// 检查统计信息是否正确 - 在输出之前检查
		if cacheStats.WindowStats.SampleCount == 0 {
			t.Fatalf("无法添加统计数据")
		}

		// 手动触发输出
		cacheStats.OutputStats()

		// 检查CSV文件是否创建
		files, err := filepath.Glob(filepath.Join(statsDir, "cachetrie_stats_*.csv"))
		if err != nil || len(files) == 0 {
			t.Fatalf("未创建CSV文件，错误: %v", err)
		}
	})

	// 测试断点续跑功能
	t.Run("ContinueFromLastBlock", func(t *testing.T) {
		// 首先创建一个统计器并输出一些数据
		stats1 := NewTrieStatsAggregator(statsDir, dataDir, StandardTrie)
		addTestTrieStats(stats1, 1) // 只添加1个区块
		stats1.OutputStats()

		// 然后创建一个新的统计器，应该能够从上一次的区块继续
		stats2 := NewTrieStatsAggregator(statsDir, dataDir, StandardTrie)
		if stats2.LastOutput != 1 {
			t.Fatalf("断点续跑功能失败，期望上次输出区块为 1，实际为 %d", stats2.LastOutput)
		}

		// 添加第二个区块的数据
		blockStats := TrieBlockStats{
			BlockNum:            2,
			WrittenStates:       200,
			ReadStates:          600,
			TransactionCount:    60,
			TransactionExecTime: time.Millisecond * 20,
			RootGenTime:         time.Millisecond * 10,
			DataSizeDelta:       2048,
			TrieType:            StandardTrie,
		}
		stats2.AddBlockStats(blockStats)
		stats2.OutputStats()

		// 检查是否创建了两个不同的CSV文件
		files, _ := filepath.Glob(filepath.Join(statsDir, "trie_stats_*.csv"))
		if len(files) < 2 {
			t.Fatalf("应该创建两个CSV文件，但只找到 %d 个", len(files))
		}
	})
}

// 创建一些测试文件
func createDummyFiles(t *testing.T, dir string, count, size int) {
	for i := 0; i < count; i++ {
		filename := filepath.Join(dir, fmt.Sprintf("test_file_%d.dat", i))

		// 创建文件
		file, err := os.Create(filename)
		if err != nil {
			t.Fatalf("创建测试文件失败: %v", err)
		}

		// 写入指定大小的随机数据
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(j % 256)
		}

		if _, err := file.Write(data); err != nil {
			file.Close()
			t.Fatalf("写入测试数据失败: %v", err)
		}

		file.Close()
	}
}

// 为普通Trie添加测试统计数据
func addTestTrieStats(stats *TrieStatsAggregator, endBlock uint64) {
	// 模拟从1到endBlock的区块
	for i := uint64(1); i <= endBlock; i++ {
		// 创建测试统计数据
		blockStats := TrieBlockStats{
			BlockNum:            i,
			WrittenStates:       100 + int(i%100),                          // 写入状态数量
			ReadStates:          500 + int(i%200),                          // 读取状态数量
			TransactionCount:    50 + int(i%50),                            // 交易数量
			TransactionExecTime: time.Millisecond * time.Duration(10+i%50), // 交易执行时间
			RootGenTime:         time.Millisecond * time.Duration(5+i%30),  // 根生成时间
			DataSizeDelta:       1024 * (int64(i) % 10),                    // 数据大小变化
			TrieType:            StandardTrie,
		}

		// 添加统计数据
		stats.AddBlockStats(blockStats)

		// 仅添加一个样本进行测试，避免大量数据
		if i >= 1 {
			break
		}
	}

	// 打印当前的样本计数
	fmt.Printf("StandardTrie 样本计数: %d\n", stats.WindowStats.SampleCount)
}

// 为CacheTrie添加测试统计数据
func addTestCacheTrieStats(stats *TrieStatsAggregator, endBlock uint64) {
	// 模拟从1到endBlock的区块
	for i := uint64(1); i <= endBlock; i++ {
		// 创建测试统计数据
		blockStats := TrieBlockStats{
			BlockNum:            i,
			WrittenStates:       100 + int(i%100),                          // 写入状态数量
			ReadStates:          500 + int(i%200),                          // 读取状态数量
			TransactionCount:    50 + int(i%50),                            // 交易数量
			TransactionExecTime: time.Millisecond * time.Duration(10+i%50), // 交易执行时间
			RootGenTime:         time.Millisecond * time.Duration(5+i%30),  // 根生成时间
			DataSizeDelta:       1024 * (int64(i) % 10),                    // 数据大小变化
			TrieType:            CacheTrie,
			CacheHitRate:        float64(50 + i%50),                       // 缓存命中率
			MemorySizeMB:        float64(10 + i%100),                      // 内存大小
			CacheSize:           1000 + int(i%1000),                       // 缓存大小
			CacheThreshold:      2000 + int(i%1000),                       // 缓存阈值
			CleanupTime:         time.Millisecond * time.Duration(1+i%20), // 清理时间
		}

		// 添加统计数据
		stats.AddBlockStats(blockStats)

		// 仅添加一个样本进行测试，避免大量数据
		if i >= 1 {
			break
		}
	}

	// 打印当前的样本计数
	fmt.Printf("CacheTrie 样本计数: %d, 缓存样本计数: %d\n",
		stats.WindowStats.SampleCount, stats.WindowStats.CacheSampleCount)
}
