package tree_test

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

// 测试配置
const (
	// 方法一配置
	method1BatchSize = 5000     // 每批次处理的数据量
	method1TotalData = 20000000 // 总数据量（可以根据需要调整到10亿）

	// 方法二配置
	method2BatchSize  = 5000 // 每批次写入的数据量
	method2Iterations = 4000 // 迭代次数

	mptDir = "F:\\ethdata\\stree2\\mpt"
)

// 数据库键
var (
	lastRootKey = []byte("last_root")
)

// 生成随机数据
func generateRandomData() ([]byte, []byte) {
	key := make([]byte, 32)
	value := make([]byte, 32)
	rand.Read(key)
	rand.Read(value)
	return key, value
}

// 生成随机数据
func generateIndexData(s int) ([]byte, []byte) {
	value := make([]byte, 32)
	key := indexKey(s)
	rand.Read(value)
	return key[:], value
}

func indexKey(s int) [32]byte {
	return sha256.Sum256([]byte(strconv.Itoa(s)))
}

// 保存最后一个根哈希到数据库
func saveLastRoot(db ethdb.Database, root common.Hash) error {
	return db.Put(lastRootKey, root.Bytes())
}

// 从数据库读取最后一个根哈希
func loadLastRoot(db ethdb.Database) (common.Hash, error) {
	data, err := db.Get(lastRootKey)
	if err != nil {
		return common.Hash{}, nil // 如果不存在，返回空哈希
	}
	return common.BytesToHash(data), nil
}

func BenchmarkMPT_Update(b *testing.B) {
	// 创建内存数据库
	memDB := rawdb.NewMemoryDatabase()
	trieDB := triedb.NewDatabase(memDB, nil)

	// 创建新的MPT树
	tr, err := trie.New(trie.TrieID(common.Hash{}), trieDB)
	if err != nil {
		b.Fatalf("创建新Trie失败: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	startTime := time.Now()
	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		tr.Update([]byte(s), []byte(s))
	}
	insertDuration := time.Since(startTime)

	// 获取根哈希
	root, _ := tr.Commit(false)

	b.Logf("插入 %d 个键值对耗时: %v (平均每个: %v), 最终根哈希: %x",
		b.N, insertDuration, insertDuration/time.Duration(b.N), root)
}

// 方法一：批量写入并提交
func TestMethod1(t *testing.T) {
	// 创建临时目录
	os.MkdirAll(mptDir, os.ModePerm)

	// 创建数据库
	ldb, err := leveldb.New(mptDir, 128, 128, "sliding-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	// 创建Trie数据库
	diskDB := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(diskDB, nil)

	// 读取最后一个根哈希
	lastRoot, err := loadLastRoot(diskDB)
	if err != nil {
		t.Fatalf("读取最后一个根哈希失败: %v", err)
	}

	// 创建Trie
	tr, err := trie.New(trie.TrieID(lastRoot), trieDB)
	if err != nil {
		t.Fatalf("创建Trie失败: %v", err)
	}

	var finalRoot common.Hash
	totalStart := time.Now()

	// 批量写入数据
	for i := 0; i < method1TotalData; i += method1BatchSize {
		batchStart := time.Now()
		batchSize := method1BatchSize
		if i+method1BatchSize > method1TotalData {
			batchSize = method1TotalData - i
		}

		// 写入数据
		for j := 0; j < batchSize; j++ {
			key, value := generateIndexData(i + j)
			tr.Update(key, value)
		}

		// 提交并获取根哈希
		root, nodes := tr.Commit(false)
		finalRoot = root

		// 更新数据库
		if err := trieDB.Update(root, common.Hash{}, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			t.Fatalf("更新数据库失败: %v", err)
		}
		if err := trieDB.Commit(root, false); err != nil {
			t.Fatalf("提交数据库失败: %v", err)
		}

		// 保存最后一个根哈希
		if err := saveLastRoot(diskDB, root); err != nil {
			t.Fatalf("保存最后一个根哈希失败: %v", err)
		}

		// 创建新的Trie继续写入
		tr, err = trie.New(trie.TrieID(root), trieDB)
		if err != nil {
			t.Fatalf("创建新Trie失败: %v", err)
		}

		batchTime := time.Since(batchStart)
		t.Logf("批次 %d-%d 完成，耗时: %v，根哈希: %x", i, i+batchSize, batchTime, root)
	}

	totalTime := time.Since(totalStart)
	t.Logf("方法一测试完成，总耗时: %v，最终根哈希: %x", totalTime, finalRoot)
}

// 方法二：基于已有根哈希进行多次小批量写入测试
func TestMethod2(t *testing.T) {
	// 创建数据库
	ldb, err := leveldb.New(mptDir, 128, 128, "sliding-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	// 创建Trie数据库
	diskDB := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(diskDB, nil)

	// 读取最后一个根哈希
	lastRoot, err := loadLastRoot(diskDB)
	if err != nil {
		t.Fatalf("读取最后一个根哈希失败: %v", err)
	}

	// 创建Trie
	tr, err := trie.New(trie.TrieID(lastRoot), trieDB)
	if err != nil {
		t.Fatalf("创建Trie失败: %v", err)
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

	// 进行多次小批量写入测试
	for i := 0; i < method2Iterations; i++ {
		// 写入数据
		writeStart := time.Now()
		for j := 0; j < method2BatchSize; j++ {
			key, value := generateRandomData()
			tr.Update(key, value)
		}
		writeTime := time.Since(writeStart)
		writeTimes = append(writeTimes, writeTime)

		// 生成根哈希
		rootStart := time.Now()
		root, nodes := tr.Commit(false)
		rootTime := time.Since(rootStart)
		rootTimes = append(rootTimes, rootTime)

		// 提交到数据库
		commitStart := time.Now()
		if err := trieDB.Update(root, common.Hash{}, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			t.Fatalf("更新数据库失败: %v", err)
		}
		if err := trieDB.Commit(root, false); err != nil {
			t.Fatalf("提交数据库失败: %v", err)
		}

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

		// 创建新的Trie继续写入
		tr, err = trie.New(trie.TrieID(root), trieDB)
		if err != nil {
			t.Fatalf("创建新Trie失败: %v", err)
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
	csvFileName := fmt.Sprintf("mpt_method2_batch%d_iter%d.csv", method2BatchSize, method2Iterations)
	writeCSVFile(t, csvFileName, csvRecords)
}

// TestMPTProof 测试MPT树的证明功能
func TestMPTProof(t *testing.T) {
	// 创建内存数据库
	memDB := rawdb.NewMemoryDatabase()
	trieDB := triedb.NewDatabase(memDB, nil)

	// 创建新的MPT树
	tr, err := trie.New(trie.TrieID(common.Hash{}), trieDB)
	if err != nil {
		t.Fatalf("创建新Trie失败: %v", err)
	}

	// 插入一些测试数据
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	t.Log("开始向MPT树插入数据")
	for k, v := range testData {
		tr.Update([]byte(k), []byte(v))
	}

	root := tr.Hash()
	//// 提交获取根哈希
	//root, _ := tr.Commit(false)
	//if err := trieDB.Commit(root, false); err != nil {
	//	t.Fatalf("提交树失败: %v", err)
	//}
	//t.Logf("MPT树根哈希: %x", root)

	// 测试证明存在的key
	proofDB := trienode.NewProofSet()
	key := "key3"

	// 为特定key生成默克尔证明
	t.Logf("为key '%s' 生成证明", key)
	if err := tr.Prove([]byte(key), proofDB); err != nil {
		t.Fatalf("生成证明失败: %v", err)
	}

	// 验证证明
	value, err := trie.VerifyProof(root, []byte(key), proofDB)
	if err != nil {
		t.Fatalf("验证证明失败: %v", err)
	}

	if string(value) != testData[key] {
		t.Fatalf("验证的值不匹配，期望 %s，得到 %s", testData[key], string(value))
	}
	t.Logf("成功验证key '%s' 的证明，值为: %s，大小为: %v.", key, string(value), proofDB.DataSize())

	// 测试证明不存在的key
	nonExistingKey := "不存在的key"
	proofDB = trienode.NewProofSet()

	t.Logf("为不存在的key '%s' 生成证明", nonExistingKey)
	if err := tr.Prove([]byte(nonExistingKey), proofDB); err != nil {
		t.Fatalf("生成不存在key的证明失败: %v", err)
	}

	// 验证不存在的key的证明
	value, err = trie.VerifyProof(root, []byte(nonExistingKey), proofDB)
	if err != nil {
		t.Fatalf("验证不存在key的证明失败: %v", err)
	}

	if value != nil {
		t.Fatalf("不存在的key应返回nil值，但得到了: %s", string(value))
	}
	t.Logf("成功验证key '%s' 不存在于树中", nonExistingKey)
}

// TestLargeMPTProof 测试大规模MPT树的证明功能
func TestLargeMPTProof(t *testing.T) {
	// 测试数据量
	const (
		totalItems = 10000000 // 1000万数据
	)

	// 定义一系列渐进增长的证明规模
	proofSizes := []int{
		2500,   // 2.5k
		5000,   // 5k
		10000,  // 1w
		20000,  // 2w
		40000,  // 4w
		80000,  // 8w
		160000, // 16w
	}

	// 创建内存数据库
	memDB := rawdb.NewMemoryDatabase()
	trieDB := triedb.NewDatabase(memDB, nil)

	// 创建新的MPT树
	tr, err := trie.New(trie.TrieID(common.Hash{}), trieDB)
	if err != nil {
		t.Fatalf("创建新Trie失败: %v", err)
	}

	// 生成并插入1000万随机数据
	t.Log("开始向MPT树插入1000万随机数据...")
	start := time.Now()

	// 保存所有键，以便后续生成证明
	allKeys := make([][]byte, totalItems)

	batchSize := 500000 // 每批50万数据
	for i := 0; i < totalItems; i++ {
		key, value := generateRandomData()
		tr.Update(key, value)
		allKeys[i] = key

		// 每插入一批数据打印进度
		if (i+1)%batchSize == 0 {
			elapsed := time.Since(start)
			t.Logf("已插入 %d 条数据 (%.2f%%)，耗时: %v", i+1, float64(i+1)*100/float64(totalItems), elapsed)
		}
	}

	insertTime := time.Since(start)
	t.Logf("插入 %d 条数据完成，总耗时: %v", totalItems, insertTime)

	// 计算树的哈希
	hashStart := time.Now()
	root := tr.Hash()
	hashTime := time.Since(hashStart)
	t.Logf("计算MPT树根哈希完成，耗时: %v，根哈希: %x", hashTime, root)

	// 随机选择不同数量的键进行证明
	t.Log("开始生成不同数量的证明...")

	// 洗牌算法，随机打乱所有键
	rand.Shuffle(len(allKeys), func(i, j int) {
		allKeys[i], allKeys[j] = allKeys[j], allKeys[i]
	})

	// 用于存储每个规模的证明结果数据
	type ProofResult struct {
		proofCount  int           // 证明数量
		elapsed     time.Duration // 总耗时
		dataSize    uint64        // 数据大小
		avgTime     time.Duration // 平均每个证明时间
		bytesPerKey float64       // 每个证明的平均字节数
		keyCount    uint64
	}

	results := make([]ProofResult, len(proofSizes))

	// 测试不同数量的证明
	for i, size := range proofSizes {
		result := generateAndMeasureProofs(t, tr, root, allKeys[:size], size)
		results[i] = result
	}

	// 打印汇总表格
	t.Log("\n证明规模性能对比:")
	t.Log("------------------------------------------------------------------------------------------------------------------------------")
	t.Log("  证明数量   |   总耗时    |   数据大小   |   节点数量   |  平均时间/证明  |  平均字节/证明  |  数量增长比例  |  大小增长比例  ")
	t.Log("------------------------------------------------------------------------------------------------------------------------------")

	// 打印第一行
	firstResult := results[0]
	t.Logf("  %-10d |  %-10v |  %-10v |  %-10v |  %-14v |  %-14.2f |       -       |       -      ",
		firstResult.proofCount, firstResult.elapsed, bytesToReadable(firstResult.dataSize), firstResult.keyCount,
		firstResult.avgTime, firstResult.bytesPerKey)
	// 打印其余行并计算增长比例
	for i := 1; i < len(results); i++ {
		current := results[i]
		//previous := results[i-1]

		countRatio := float64(current.proofCount) / float64(firstResult.proofCount)
		sizeRatio := float64(current.dataSize) / float64(firstResult.dataSize)

		t.Logf("  %-10d |  %-10v |  %-10v |  %-10v |  %-14v |  %-14.2f |    %-10.2fx |    %-10.2fx",
			current.proofCount, current.elapsed, bytesToReadable(current.dataSize), current.keyCount,
			current.avgTime, current.bytesPerKey, countRatio, sizeRatio)
	}
	t.Log("------------------------------------------------------------------------------------------------------------------------------")
}

// TestHugeMPTProof 基于数据库的数据进行测试
func TestHugeMPTProof(t *testing.T) {
	// 创建数据库
	ldb, err := leveldb.New(mptDir, 128, 128, "sliding-test", false)
	if err != nil {
		t.Fatalf("创建数据库失败: %v", err)
	}
	defer ldb.Close()

	root := common.HexToHash("0x8411668e6c14ecf88e2a79c5eaf478fd87829b0a6df3f9c7de6d02081dc57398")

	// 创建Trie数据库
	diskDB := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(diskDB, nil)

	// 创建新的MPT树
	tr, err := trie.New(trie.TrieID(root), trieDB)
	if err != nil {
		t.Fatalf("创建新Trie失败: %v", err)
	}

	proofSizes := []int{12000, 20000, 40000, 80000, 160000, 240000, 320000}

	// 保存所有键，以便后续生成证明
	allKeys := make([][]byte, 10000000)
	for i := 0; i < 10000000; i++ {
		allKeys[i], _ = generateIndexData(i)
	}

	// 洗牌算法，随机打乱所有键
	rand.Shuffle(len(allKeys), func(i, j int) {
		allKeys[i], allKeys[j] = allKeys[j], allKeys[i]
	})

	// 用于存储每个规模的证明结果数据
	type ProofResult struct {
		proofCount  int           // 证明数量
		elapsed     time.Duration // 总耗时
		dataSize    uint64        // 数据大小
		avgTime     time.Duration // 平均每个证明时间
		bytesPerKey float64       // 每个证明的平均字节数
		keyCount    uint64
	}

	results := make([]ProofResult, len(proofSizes))

	// 测试不同数量的证明
	for i, size := range proofSizes {
		result := generateAndMeasureProofs(t, tr, root, allKeys[:size], size)
		results[i] = result
	}

	// 打印汇总表格
	t.Log("\n证明规模性能对比:")
	t.Log("------------------------------------------------------------------------------------------------------------------------------")
	t.Log("  证明数量   |   总耗时    |   数据大小   |   节点数量   |  平均时间/证明  |  平均字节/证明  |  数量增长比例  |  大小增长比例  ")
	t.Log("------------------------------------------------------------------------------------------------------------------------------")

	// 打印第一行
	firstResult := results[0]
	t.Logf("  %-10d |  %-10v |  %-10v |  %-10v |  %-14v |  %-14.2f |       -       |       -      ",
		firstResult.proofCount, firstResult.elapsed, bytesToReadable(firstResult.dataSize), firstResult.keyCount,
		firstResult.avgTime, firstResult.bytesPerKey)
	// 打印其余行并计算增长比例
	for i := 1; i < len(results); i++ {
		current := results[i]
		//previous := results[i-1]

		countRatio := float64(current.proofCount) / float64(firstResult.proofCount)
		sizeRatio := float64(current.dataSize) / float64(firstResult.dataSize)

		t.Logf("  %-10d |  %-10v |  %-10v |  %-10v |  %-14v |  %-14.2f |    %-10.2fx |    %-10.2fx",
			current.proofCount, current.elapsed, bytesToReadable(current.dataSize), current.keyCount,
			current.avgTime, current.bytesPerKey, countRatio, sizeRatio)
	}
	t.Log("------------------------------------------------------------------------------------------------------------------------------")
}

// generateAndMeasureProofs 生成特定数量的证明并测量性能
func generateAndMeasureProofs(t *testing.T, tr *trie.Trie, root common.Hash, keys [][]byte, size int) struct {
	proofCount  int
	elapsed     time.Duration
	dataSize    uint64
	avgTime     time.Duration
	bytesPerKey float64
	keyCount    uint64
} {
	proofDB := trienode.NewProofSet()

	start := time.Now()
	for i, key := range keys {
		if err := tr.Prove(key, proofDB); err != nil {
			t.Fatalf("生成证明失败: %v", err)
		}

		// 每生成1万个证明打印一次进度
		if (i+1)%10000 == 0 {
			t.Logf("已生成 %d/%d 个证明...", i+1, len(keys))
		}
	}
	elapsed := time.Since(start)

	// 验证一个随机证明
	randomIndex := rand.Intn(len(keys))
	randomKey := keys[randomIndex]
	value, err := trie.VerifyProof(root, randomKey, proofDB)
	if err != nil {
		t.Fatalf("验证证明失败: %v", err)
	}

	keyCount := uint64(proofDB.KeyCount())
	dataSize := uint64(proofDB.DataSize())
	avgTime := elapsed / time.Duration(len(keys))
	bytesPerKey := float64(dataSize) / float64(len(keys))

	sizeText := formatSize(size)
	t.Logf("生成 %s 个证明完成，总耗时: %v，平均每个: %v，证明数据总大小: %v (%.2f 字节/证明)",
		sizeText, elapsed, avgTime, bytesToReadable(dataSize), bytesPerKey)
	t.Logf("随机验证第 %d 个证明成功，键长度: %d，值长度: %d",
		randomIndex, len(randomKey), len(value))

	return struct {
		proofCount  int
		elapsed     time.Duration
		dataSize    uint64
		avgTime     time.Duration
		bytesPerKey float64
		keyCount    uint64
	}{
		proofCount:  len(keys),
		elapsed:     elapsed,
		dataSize:    dataSize,
		avgTime:     avgTime,
		bytesPerKey: bytesPerKey,
		keyCount:    keyCount,
	}
}

// formatSize 格式化数字为易读的文本
func formatSize(size int) string {
	if size < 1000 {
		return fmt.Sprintf("%d", size)
	} else if size < 1000000 {
		return fmt.Sprintf("%.1fk", float64(size)/1000)
	} else {
		return fmt.Sprintf("%.1fw", float64(size)/10000)
	}
}

// bytesToReadable 将字节数转换为可读格式
func bytesToReadable(bytes uint64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	if bytes < KB {
		return fmt.Sprintf("%d B", bytes)
	} else if bytes < MB {
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	} else if bytes < GB {
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	} else {
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	}
}
