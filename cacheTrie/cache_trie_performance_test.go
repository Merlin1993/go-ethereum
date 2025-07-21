package cacheTrie

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// 写入模式枚举
type WriteMode int

const (
	ModeNormal    WriteMode = iota // 正常模式
	ModeSpike10x                   // 模式一：每200次写入，强度增加10倍，持续10次
	ModeSpike2x                    // 模式二：每200次写入，强度增加2倍，持续200次
	ModeGradual10                  // 模式三：每200次写入增加10%（不是复利）
)

func TestCachePerformance(t *testing.T) {
	stateCount := 5000
	iterationCount := 4000 // 统计循环次数
	maxSize := 1000000     // 初始存储大小限制
	windowMultiple := 256
	// 测试正常模式
	t.Run(fmt.Sprintf("NormalMode_StateCount_%d", stateCount), func(t *testing.T) {
		testCacheTrieWithData(t, stateCount, iterationCount, windowMultiple, maxSize, ModeNormal)
	})
	//
	//测试模式一：尖峰10倍模式
	t.Run(fmt.Sprintf("Spike10xMode_StateCount_%d", stateCount), func(t *testing.T) {
		testCacheTrieWithData(t, stateCount, iterationCount, windowMultiple, maxSize, ModeSpike10x)
	})

	// 测试模式二：尖峰2倍模式
	t.Run(fmt.Sprintf("Spike2xMode_StateCount_%d", stateCount), func(t *testing.T) {
		testCacheTrieWithData(t, stateCount, iterationCount, windowMultiple, maxSize, ModeSpike2x)
	})

	// 测试模式三：渐进10%模式
	t.Run(fmt.Sprintf("Gradual10pMode_StateCount_%d", stateCount), func(t *testing.T) {
		testCacheTrieWithData(t, stateCount, iterationCount, windowMultiple, maxSize, ModeGradual10)
	})
}

// 计算当前迭代的写入强度倍数
func calculateWriteIntensity(iteration, baseStateCount int, mode WriteMode) int {
	switch mode {
	case ModeNormal:
		return 100
	case ModeSpike10x:
		// 每200次写入，强度增加10倍，持续10次
		positionInCycle := iteration % 350
		if positionInCycle < 50 {
			return 1000
		}
		return 100
	case ModeSpike2x:
		// 每200次写入，强度增加2倍，持续200次
		positionInCycle := iteration % 200
		if positionInCycle < 200 {
			return 200
		}
		return 100
	case ModeGradual10:
		// 每200次写入增加10%（不是复利）
		cycles := iteration / 400
		return 100 + cycles*20
	default:
		return 100
	}
}

// testCacheTrieWithStateCount 使用指定状态数进行CacheTrie测试
func testCacheTrieWithData(t *testing.T, stateCount, iterationCount, windowMultiple, maxSize int, mod WriteMode) {
	t.Logf("开始测试: 单次写入状态数=%d, 统计循环次数=%d, 初始存储大小=%d", stateCount, iterationCount, maxSize)

	// 创建CacheTrie实例
	cacheTrie := NewCacheTrie(startBlockNum, uint64(windowMultiple), maxSize)

	// 预热阶段 - 执行到第一次清理
	t.Log("开始预热阶段...")
	preWarmupStartTime := time.Now()

	// 设置初始区块高度
	currentBlock := uint64(startBlockNum)
	cacheTrie.SetBlockNum(currentBlock)

	// 记录初始状态
	initialHRW := cacheTrie.GetHRW()
	initialThreshold := initialHRW.GetThreshold()
	t.Logf("初始状态: 阈值(ssthresh)=%d", initialThreshold)

	// 进行预热，直到发生第一次清理
	warmupBatchSize := 10000 // 每批次写入数量
	warmupBatches := 0

	for i := 0; i < warmupStateCount; i += warmupBatchSize {
		batchSize := warmupBatchSize
		if i+warmupBatchSize > warmupStateCount {
			batchSize = warmupStateCount - i
		}

		// 写入数据
		for j := 0; j < batchSize; j++ {
			key, value := generateRandomData()
			cacheTrie.Update(key, value, true)
		}

		// 获取哈希，这会触发清理机制
		hash, _, kvList := cacheTrie.Hash()

		if kvList != nil && len(kvList.Data) > 0 {
			go func() {
				cacheTrie.FinishCleanup(currentBlock, hash)
			}()
		}

		currentBlock++
		cacheTrie.SetBlockNum(currentBlock)

		warmupBatches++

		// 检查是否发生了清理
		currentCleanupCount := cacheTrie.GetCleanupCount()
		if currentCleanupCount > 0 {
			t.Logf("预热阶段检测到清理发生，批次数=%d, 写入状态数=%d", warmupBatches, (warmupBatches-1)*warmupBatchSize+batchSize)
			break
		}

		// 如果写入了太多数据还没有触发清理，可以提前结束预热
		if i+batchSize >= warmupStateCount {
			t.Logf("预热阶段结束，未检测到清理发生，已写入状态数=%d", i+batchSize)
		}
	}
	CleanupTime = 0

	preWarmupDuration := time.Since(preWarmupStartTime)
	t.Logf("预热阶段完成，耗时: %v", preWarmupDuration)

	// 正式测试阶段
	t.Log("开始正式测试阶段...")

	// 记录每次操作的统计数据
	type IterationStats struct {
		WriteTime       time.Duration // 写入耗时
		HashTime        time.Duration // 计算哈希耗时
		WriteSpeed      float64       // 写入速度（状态/秒）
		Size            int           // 当前size
		Threshold       int           // 当前阈值
		CleanupOccurred bool          // 是否发生清理
		CleanupTime     time.Duration // 清理耗时（如果发生）
		CleanSize       int
	}

	stats := make([]IterationStats, iterationCount)

	// 准备CSV数据
	csvRecords := [][]string{
		{"迭代", "写入耗时(ns)", "哈希耗时(ns)", "写入速度(状态/秒)", "Size", "Threshold", "是否清理", "清理耗时(ns)", "清理数量"},
	}

	// 重置清理时间统计
	cacheTrie.ResetCleanupTimes()

	for i := 0; i < iterationCount; i++ {
		// 记录写入开始时间
		writeStart := time.Now()
		newStateCount := stateCount * calculateWriteIntensity(i, stateCount, mod) / 100

		// 写入指定数量的状态
		for j := 0; j < newStateCount; j++ {
			// 50%概率使用普通键值，50%概率使用带地址的键值
			if rand.Intn(2) == 0 {
				key, value := generateRandomData()
				cacheTrie.Update(key, value, true)
			} else {
				addr := generateRandomAddress()
				key, value := generateRandomData()
				cacheTrie.UpdateWithAddress(addr, key, value, true)
			}
		}

		writeTime := time.Since(writeStart)

		// 记录当前清理计数
		beforeHashCleanupCount := cacheTrie.GetCleanupCount()
		beforeCleanupTotalTime, _ := cacheTrie.GetCleanupTimes()

		// 获取哈希，这会触发清理机制
		hashStart := time.Now()
		hash, _, kvList := cacheTrie.Hash()
		//这步操作不会影响到hash值，所以也可以后面再操作
		hashTime := time.Since(hashStart) - CleanupTime
		CleanupTime = 0

		if kvList != nil {
			go func() {
				cacheTrie.FinishCleanup(currentBlock, hash)
			}()
		}

		// 移动到下一个区块
		currentBlock++
		cacheTrie.SetBlockNum(currentBlock)

		// 检查是否发生了清理
		afterHashCleanupCount := cacheTrie.GetCleanupCount()
		cleanupOccurred := afterHashCleanupCount > beforeHashCleanupCount

		// 计算清理耗时（如果发生）
		var cleanupTime time.Duration
		if cleanupOccurred {
			afterCleanupTotalTime, _ := cacheTrie.GetCleanupTimes()
			cleanupTime = afterCleanupTotalTime - beforeCleanupTotalTime
		}

		// 计算写入速度（状态/秒）
		writeSpeed := float64(stateCount) / writeTime.Seconds()

		// 获取当前size和threshold
		currentSize := cacheTrie.GetSize()
		currentThreshold := cacheTrie.GetHRW().GetAllSize()

		cleanSize := 0
		if kvList != nil && len(kvList.Data) > 0 {
			cleanSize = len(kvList.Data)
		}
		// 保存统计信息
		stats[i] = IterationStats{
			WriteTime:       writeTime,
			HashTime:        hashTime,
			WriteSpeed:      writeSpeed,
			Size:            currentSize,
			Threshold:       currentThreshold,
			CleanupOccurred: cleanupOccurred,
			CleanupTime:     cleanupTime,
			CleanSize:       cleanSize,
		}

		// 添加到CSV记录
		csvRecords = append(csvRecords, []string{
			strconv.Itoa(i + 1),
			strconv.FormatInt(writeTime.Nanoseconds(), 10),
			strconv.FormatInt(hashTime.Nanoseconds(), 10),
			strconv.FormatFloat(writeSpeed, 'f', 2, 64),
			strconv.Itoa(currentSize),
			strconv.Itoa(currentThreshold),
			strconv.FormatBool(cleanupOccurred),
			strconv.FormatInt(cleanupTime.Nanoseconds(), 10),
		})

		// 输出当前迭代的统计信息
		cleanupStatus := "无"
		if cleanupOccurred {
			cleanupStatus = fmt.Sprintf("发生，耗时: %v", cleanupTime)
		}

		t.Logf("迭代 %d/%d: 写入耗时=%v, 速度=%.2f 状态/秒, 哈希耗时=%v, Size=%d, Threshold=%d, 清理: %s",
			i+1, iterationCount, writeTime, writeSpeed, hashTime.String(), currentSize, currentThreshold, cleanupStatus)
	}

	// 计算平均统计数据
	var totalWriteTime, totalHashTime, totalCleanupTime time.Duration
	var totalWriteSpeed float64
	cleanupCount := 0

	for _, stat := range stats {
		totalWriteTime += stat.WriteTime
		totalHashTime += stat.HashTime
		totalWriteSpeed += stat.WriteSpeed

		if stat.CleanupOccurred {
			cleanupCount++
			totalCleanupTime += stat.CleanupTime
		}
	}

	avgWriteTime := totalWriteTime / time.Duration(iterationCount)
	avgHashTime := totalHashTime / time.Duration(iterationCount)
	avgWriteSpeed := totalWriteSpeed / float64(iterationCount)

	var avgCleanupTime time.Duration
	if cleanupCount > 0 {
		avgCleanupTime = totalCleanupTime / time.Duration(cleanupCount)
	}

	// 输出汇总统计信息
	t.Logf("\n===== 测试汇总 (状态数: %d) =====", stateCount)
	t.Logf("平均写入耗时: %v", avgWriteTime)
	t.Logf("平均写入速度: %.2f 状态/秒", avgWriteSpeed)
	t.Logf("平均哈希耗时: %v", avgHashTime)
	t.Logf("触发清理次数: %d/%d", cleanupCount, iterationCount)

	if cleanupCount > 0 {
		t.Logf("平均清理耗时: %v", avgCleanupTime)
	}

	// 获取最终的hit/miss统计
	totalGetRequests, hitCount, missCount, getHitRate,
		totalUpdateRequests, updateHitCount, updateMissCount, updateHitRate := cacheTrie.GetHitRate()

	t.Logf("Get操作: 总请求=%d, 命中=%d, 未命中=%d, 命中率=%.2f%%",
		totalGetRequests, hitCount, missCount, getHitRate*100)
	t.Logf("Update操作: 总请求=%d, 命中=%d, 未命中=%d, 命中率=%.2f%%",
		totalUpdateRequests, updateHitCount, updateMissCount, updateHitRate*100)

	// 获取内存占用信息
	memSize := cacheTrie.GetMemorySize()
	t.Logf("内存占用: %d 字节 (%.2f MB)", memSize, float64(memSize)/(1024*1024))

	// 将结果写入CSV文件
	csvFileName := fmt.Sprintf("cacheTrie_states%d_iter%d_window%d_maxsize%d_mod%d.csv",
		stateCount, iterationCount, windowMultiple, maxSize, mod)
	writeCSVFile(t, csvFileName, csvRecords)

	// 汇总统计添加到摘要CSV
	writeCSVSummary(t, stateCount, iterationCount, windowMultiple, maxSize,
		avgWriteTime, avgHashTime, avgWriteSpeed, cleanupCount,
		avgCleanupTime, getHitRate, updateHitRate, uint64(memSize))
}
