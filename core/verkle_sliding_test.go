package core

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
)

// 测试配置
const (
	verkleDir = "F:\\ethdata\\stree2\\verkle"
)

// 方法一：批量写入并提交
func TestVerkleMethod1(t *testing.T) {
	// 创建临时目录
	os.MkdirAll(verkleDir, os.ModePerm)

	// 创建数据库
	ldb, err := leveldb.New(verkleDir, 128, 128, "verkle-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	// 创建Trie数据库
	cacheConfig := DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	diskDB := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(diskDB, cacheConfig.triedbConfig(true))

	// 读取最后一个根哈希
	lastRoot, err := loadLastRoot(diskDB)
	if err != nil {
		t.Fatalf("读取最后一个根哈希失败: %v", err)
	}
	t.Logf("读取最后一个根哈希：%v", lastRoot.String())

	// 创建point cache
	pointCache := utils.NewPointCache(1024)

	// 创建Verkle Trie
	vt, err := trie.NewVerkleTrie(lastRoot, trieDB, pointCache)
	if err != nil {
		t.Fatalf("创建Verkle Trie失败: %v", err)
	}

	var finalRoot common.Hash
	totalStart := time.Now()

	// 使用固定的地址进行测试
	testAddr := common.Address{}

	// 批量写入数据
	for i := 0; i < method1TotalData; i += method1BatchSize {
		batchStart := time.Now()
		batchSize := method1BatchSize
		if i+method1BatchSize > method1TotalData {
			batchSize = method1TotalData - i
		}

		// 写入数据
		for j := 0; j < batchSize; j++ {
			key, value := generateRandomData()
			if err := vt.UpdateStorage(testAddr, key, value); err != nil {
				t.Fatalf("更新存储数据失败: %v", err)
			}
		}

		// 提交并获取根哈希
		root, nodes := vt.Commit(false)

		// 更新数据库
		mergedNodeset := trienode.NewWithNodeSet(nodes)
		stateSet := triedb.NewStateSet()

		if err := trieDB.Update(root, finalRoot, uint64(i), mergedNodeset, stateSet); err != nil {
			t.Fatalf("更新数据库失败: %v", err)
		}
		if err := trieDB.Commit(root, false); err != nil {
			t.Fatalf("提交数据库失败: %v", err)
		}

		finalRoot = root
		// 保存最后一个根哈希
		if err := saveLastRoot(diskDB, root); err != nil {
			t.Fatalf("保存最后一个根哈希失败: %v", err)
		}

		// 创建新的Verkle Trie继续写入
		vt, err = trie.NewVerkleTrie(root, trieDB, pointCache)
		if err != nil {
			t.Fatalf("创建新Verkle Trie失败: %v", err)
		}

		batchTime := time.Since(batchStart)
		t.Logf("批次 %d-%d 完成，耗时: %v，根哈希: %x", i, i+batchSize, batchTime, root)
	}

	totalTime := time.Since(totalStart)
	t.Logf("方法一测试完成，总耗时: %v，最终根哈希: %x", totalTime, finalRoot)
}

// 方法二：基于已有根哈希进行多次小批量写入测试
func TestVerkleMethod2(t *testing.T) {
	// 创建数据库
	ldb, err := leveldb.New(verkleDir, 128, 128, "verkle-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	// 创建Trie数据库
	cacheConfig := DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	diskDB := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(diskDB, cacheConfig.triedbConfig(true))

	// 读取最后一个根哈希
	lastRoot, err := loadLastRoot(diskDB)
	if err != nil {
		t.Fatalf("读取最后一个根哈希失败: %v", err)
	}

	// 创建point cache
	pointCache := utils.NewPointCache(1024)

	// 创建Verkle Trie
	vt, err := trie.NewVerkleTrie(lastRoot, trieDB, pointCache)
	if err != nil {
		t.Fatalf("创建Verkle Trie失败: %v", err)
	}

	// 记录每次操作的耗时
	var (
		writeTimes  []time.Duration
		commitTimes []time.Duration
		rootTimes   []time.Duration
	)

	// 准备CSV数据
	csvRecords := [][]string{
		{"迭代", "写入耗时(ns)", "生成根耗时(ns)", "提交耗时(ns)", "根哈希"},
	}

	// 使用固定的地址进行测试
	testAddr := common.Address{}

	// 进行多次小批量写入测试
	for i := 0; i < method2Iterations; i++ {
		// 写入数据
		writeStart := time.Now()
		for j := 0; j < method2BatchSize; j++ {
			key, value := generateRandomData()
			if err := vt.UpdateStorage(testAddr, key, value); err != nil {
				t.Fatalf("更新存储数据失败: %v", err)
			}
		}
		writeTime := time.Since(writeStart)
		writeTimes = append(writeTimes, writeTime)

		// 生成根哈希
		rootStart := time.Now()
		root, nodes := vt.Commit(false)
		rootTime := time.Since(rootStart)
		rootTimes = append(rootTimes, rootTime)

		// 提交到数据库
		commitStart := time.Now()
		mergedNodeset := trienode.NewWithNodeSet(nodes)
		stateSet := triedb.NewStateSet()

		if err := trieDB.Update(root, lastRoot, 0, mergedNodeset, stateSet); err != nil {
			t.Fatalf("更新数据库失败: %v", err)
		}
		if err := trieDB.Commit(root, false); err != nil {
			t.Fatalf("提交数据库失败: %v", err)
		}
		lastRoot = root

		// 保存最后一个根哈希
		if err := saveLastRoot(diskDB, root); err != nil {
			t.Fatalf("保存最后一个根哈希失败: %v", err)
		}

		commitTime := time.Since(commitStart)
		commitTimes = append(commitTimes, commitTime)

		// 将数据添加到CSV记录
		csvRecords = append(csvRecords, []string{
			strconv.Itoa(i + 1),
			strconv.FormatInt(writeTime.Nanoseconds(), 10),
			strconv.FormatInt(rootTime.Nanoseconds(), 10),
			strconv.FormatInt(commitTime.Nanoseconds(), 10),
			root.Hex(),
		})

		// 创建新的Verkle Trie继续写入
		vt, err = trie.NewVerkleTrie(root, trieDB, pointCache)
		if err != nil {
			t.Fatalf("创建新Verkle Trie失败: %v", err)
		}

		t.Logf("迭代 %d 完成，写入耗时: %v，生成根耗时: %v，提交耗时: %v，根哈希: %x",
			i+1, writeTime, rootTime, commitTime, root)
	}

	// 计算平均耗时
	var avgWrite, avgRoot, avgCommit time.Duration
	for i := 0; i < method2Iterations; i++ {
		avgWrite += writeTimes[i]
		avgRoot += rootTimes[i]
		avgCommit += commitTimes[i]
	}
	avgWrite /= time.Duration(method2Iterations)
	avgRoot /= time.Duration(method2Iterations)
	avgCommit /= time.Duration(method2Iterations)

	t.Logf("方法二测试完成，平均耗时 - 写入: %v，生成根: %v，提交: %v",
		avgWrite, avgRoot, avgCommit)

	// 将结果写入CSV文件
	csvFileName := fmt.Sprintf("verkle_method2_batch%d_iter%d.csv", method2BatchSize, method2Iterations)
	writeCSVFile(t, csvFileName, csvRecords)
}

// BenchmarkVT_Update 测试Verkle树插入性能
func BenchmarkVT_Update(b *testing.B) {
	// 创建内存数据库
	memDB := rawdb.NewMemoryDatabase()

	// 创建Trie数据库配置
	cacheConfig := DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	trieDB := triedb.NewDatabase(memDB, cacheConfig.triedbConfig(true))

	// 创建新的Verkle Trie (不使用point cache)
	vt, err := trie.NewVerkleTrie(common.Hash{}, trieDB, nil)
	if err != nil {
		b.Fatalf("创建新Verkle Trie失败: %v", err)
	}

	// 使用固定的地址进行测试
	testAddr := common.Address{}

	b.ResetTimer()
	b.ReportAllocs()
	startTime := time.Now()

	for i := 0; i < b.N; i++ {
		key, value := generateRandomData()
		if err := vt.UpdateStorage(testAddr, key, value); err != nil {
			b.Fatalf("更新存储数据失败: %v", err)
		}
	}

	insertDuration := time.Since(startTime)

	// 获取根哈希
	root, _ := vt.Commit(false)

	b.Logf("插入 %d 个键值对耗时: %v (平均每个: %v), 最终根哈希: %x",
		b.N, insertDuration, insertDuration/time.Duration(b.N), root)
}

// 将测试结果写入CSV文件
func writeCSVFile(t *testing.T, fileName string, records [][]string) {
	// 确保结果目录存在
	resultsDir := "results"
	if _, err := os.Stat(resultsDir); os.IsNotExist(err) {
		if err := os.Mkdir(resultsDir, 0755); err != nil {
			t.Logf("创建结果目录失败: %v", err)
			return
		}
	}

	// 创建CSV文件
	filePath := filepath.Join(resultsDir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		t.Logf("创建CSV文件失败: %v", err)
		return
	}
	defer file.Close()

	// 创建CSV写入器
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入数据
	if err := writer.WriteAll(records); err != nil {
		t.Logf("写入CSV数据失败: %v", err)
		return
	}

	t.Logf("测试结果已写入CSV文件: %s", filePath)
}
