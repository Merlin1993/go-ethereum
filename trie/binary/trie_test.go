package binary

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
)

// LevelDBAdapter adapts leveldb.Database to KVStore interface
type LevelDBAdapter struct {
	*leveldb.Database
}

func (db *LevelDBAdapter) NewBatch() Batcher {
	return db.Database.NewBatch()
}

func (db *LevelDBAdapter) PutBucket(hash []byte, data []byte) error {
	return db.Put(hash, data)
}

func (db *LevelDBAdapter) GetBucket(hash []byte) ([]byte, error) {
	return db.Get(hash)
}

func (db *LevelDBAdapter) DeleteBucket(hash []byte) error {
	return db.Delete(hash)
}

func TestTriePerformance(t *testing.T) {
	// 1. 输入参数
	BatchSize := 5000
	TotalBatches := 165536 // 总批次数
	NewRatio := 0.5        // 新增：更新 = 7:3

	// 剪枝触发配置：
	// “每完成 40 次批量新数据插入操作触发对下一个分片的剪枝”
	// 这里使用每插入 40 条触发一次剪枝，以保证测试过程剪枝持续发生并循环分片。
	PruneTriggerEvery := 1

	// 2. 测试环境：创建 LevelDB 临时目录
	// 使用当前目录 "." 作为临时文件存储基准，避免占用 C 盘系统临时目录
	// 您也可以将其修改为绝对路径，例如 "D:\\trie_perf_data"
	baseDir := "F:\\\\trie_perf_data"
	dir, err := os.MkdirTemp(baseDir, "trie-perf-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf(dir)
	defer os.RemoveAll(dir) // 测试结束清理目录

	// 启动独立 LevelDB 实例（缓存 256MB，句柄 256）
	ldb, err := leveldb.New(dir, 256, 256, "test", false)
	if err != nil {
		t.Fatal(err)
	}
	defer ldb.Close()

	// 初始化 Trie
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	dbAdapter := &LevelDBAdapter{ldb}
	config.ArchiveDB = dbAdapter
	trie := NewTrie(nil, dbAdapter, hasher, config, true)

	// 3. 测试流程：初始化全局年度基准为 0
	trie.SetGlobalEpoch(0)

	fmt.Printf("开始性能测试\n")
	fmt.Printf("单批量插入: %d，总批次: %d，新增比例: %.2f\n", BatchSize, TotalBatches, NewRatio)
	fmt.Printf("DB 路径: %s\n", dir)
	fmt.Printf("每批日志格式: 批次=<n> 插入耗时(ms)=<insert> 剪枝耗时(ms)=<prune> 提交耗时(ms)=<commit> 总批次耗时(ms)=<total> 磁盘占用(MB)=<disk>\n")

	var (
		totalInsertItems int
		totalPrunedItems int
		totalInsertTime  time.Duration
		totalPruneTime   time.Duration
		totalCommitTime  time.Duration
		totalFlushTime   time.Duration
		minDiskUsage     float64 = -1.0
		maxDiskUsage     float64 = 0.0
	)

	existingKeys := lru.NewCache[string, struct{}](100000)
	batchCount := 0

	// 循环执行
	for i := 0; i < TotalBatches; i++ {
		// 生成本批次数据
		batchKeys := make([][]byte, 0, BatchSize)
		batchVals := make([][]byte, 0, BatchSize)

		newCount := int(float64(BatchSize) * NewRatio)
		updateCount := BatchSize - newCount

		// 更新：复用已有 key
		if existingKeys.Len() > 0 {
			allKeys := existingKeys.Keys() // 本批次复用旧 key，从当前 LRU 缓存中提取
			if updateCount > len(allKeys) {
				updateCount = len(allKeys)
				newCount = BatchSize - updateCount
			}
			for j := 0; j < updateCount; j++ {
				// 随机选择旧 key 进行更新
				idx := randInt(len(allKeys))
				batchKeys = append(batchKeys, []byte(allKeys[idx]))

				val := make([]byte, 32)
				rand.Read(val)
				batchVals = append(batchVals, val)
			}
		} else {
			// 若无旧 key，则本批次全部为新增
			newCount = BatchSize
			updateCount = 0
		}

		// 新增插入
		for j := 0; j < newCount; j++ {
			k := make([]byte, 32)
			rand.Read(k)
			batchKeys = append(batchKeys, k)
			batchVals = append(batchVals, k) // 简化：值=键
			existingKeys.Add(string(k), struct{}{})
		}

		// 执行：批量插入 → 剪枝（触发时） → 提交

		// 1. 插入与剪枝
		startInsert := time.Now()
		currentPruneTime := time.Duration(0)

		for j, key := range batchKeys {
			err := trie.Put(key, batchVals[j])
			if err != nil {
				t.Fatalf("插入错误: %v", err)
			}
			totalInsertItems++
		}

		// 插入耗时
		currentInsertTime := time.Since(startInsert)

		// 检查按批次剪枝触发器
		batchCount++
		if batchCount >= PruneTriggerEvery {
			batchCount = 0
			// 每次触发剪枝前旋转 Epoch 位，确保能归档上一个周期的旧数据
			trie.SetGlobalEpoch(1 - trie.globalEpochBit)

			pStart := time.Now()
			err := trie.PruneNextShard()
			if err != nil {
				t.Fatalf("归档错误: %v", err)
			}
			currentPruneTime = time.Since(pStart)
		}

		// 2. 提交
		startCommit := time.Now()
		_, err := trie.Commit()
		if err != nil {
			t.Fatalf("提交错误: %v", err)
		}
		currentCommitTime := time.Since(startCommit)

		// 3. 归档落盘 (本次重构分离出来的 I/O)
		startFlush := time.Now()
		if err := trie.FlushArchives(); err != nil {
			t.Fatalf("归档刷盘错误: %v", err)
		}
		currentFlushTime := time.Since(startFlush)

		// 指标
		totalLoopTime := currentInsertTime + currentPruneTime + currentCommitTime + currentFlushTime
		diskUsageBytes := getDirSize(dir)
		diskUsageMB := float64(diskUsageBytes) / 1024 / 1024

		if minDiskUsage < 0 || diskUsageMB < minDiskUsage {
			minDiskUsage = diskUsageMB
		}
		if diskUsageMB > maxDiskUsage {
			maxDiskUsage = diskUsageMB
		}

		totalInsertTime += currentInsertTime
		totalPruneTime += currentPruneTime
		totalCommitTime += currentCommitTime
		totalFlushTime += currentFlushTime

		// 输出日志
		fmt.Printf("批次=%d 插入=%d 剪枝=%d 提交=%d 刷盘=%d 总计=%d 磁盘=%.2fMB\n",
			i+1,
			currentInsertTime.Milliseconds(),
			currentPruneTime.Milliseconds(),
			currentCommitTime.Milliseconds(),
			currentFlushTime.Milliseconds(),
			totalLoopTime.Milliseconds(),
			diskUsageMB,
		)
	}

	// 4. 汇总
	avgLoopTime := (totalInsertTime + totalPruneTime + totalCommitTime + totalFlushTime) / time.Duration(TotalBatches)

	fmt.Printf("\n--- 测试汇总 ---\n")
	fmt.Printf("总插入数据量: %d\n", totalInsertItems)
	fmt.Printf("总剪枝数据量: %d\n", totalPrunedItems)
	fmt.Printf("平均单次耗时: %v\n", avgLoopTime)
	fmt.Printf("最小磁盘占用: %.2f MB\n", minDiskUsage)
	fmt.Printf("最大磁盘占用: %.2f MB\n", maxDiskUsage)
}

// 计算目录大小
func getDirSize(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // 遍历错误忽略
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}

// 生成随机整数 [0, n)
func randInt(n int) int {
	b := make([]byte, 4)
	rand.Read(b)
	// 简单掩码与取模
	val := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	return int(val % uint32(n))
}
