package tree

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

// Test configuration
const (
	// Method 1 configuration
	method1BatchSize = 1_000         // Data volume processed per batch
	method1TotalData = 1_000_000_000 // Total data volume (can be adjusted to 1 billion as needed)

	// Method 2 configuration
	method2BatchSize  = 5000  // Data volume written per batch
	method2Iterations = 20000 // Number of iterations

	mptDir = "/home/ASCT/mpt_stress" // "F:\\trie_stress_data\\mpt"
)

// Database keys
var (
	lastRootKey = []byte("last_root")
)

// Generate random data
func generateRandomData() ([]byte, []byte) {
	key := make([]byte, 32)
	value := make([]byte, 32)
	rand.Read(key)
	rand.Read(value)
	return key, value
}

// Generate random data
func generateIndexData(s int) ([]byte, []byte) {
	value := make([]byte, 32)
	key := indexKey(s)
	rand.Read(value)
	return key[:], value
}

func indexKey(s int) [32]byte {
	return sha256.Sum256([]byte(strconv.Itoa(s)))
}

// Save the last root hash to database
func saveLastRoot(db ethdb.Database, root common.Hash) error {
	return db.Put(lastRootKey, root.Bytes())
}

// Load the last root hash from database
func loadLastRoot(db ethdb.Database) (common.Hash, error) {
	data, err := db.Get(lastRootKey)
	if err != nil {
		return common.Hash{}, nil // Return zero hash if not exists
	}
	return common.BytesToHash(data), nil
}

func BenchmarkMPT_Update(b *testing.B) {
	// Create in-memory database
	memDB := rawdb.NewMemoryDatabase()
	trieDB := triedb.NewDatabase(memDB, nil)

	// Create new MPT tree
	tr, err := trie.New(trie.TrieID(common.Hash{}), trieDB)
	if err != nil {
		b.Fatalf("Failed to create new Trie: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	startTime := time.Now()
	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		tr.Update([]byte(s), []byte(s))
	}
	insertDuration := time.Since(startTime)

	// Get root hash
	root, _ := tr.Commit(false)

	b.Logf("Inserted %d key-value pairs in %v (average per item: %v), final root hash: %x",
		b.N, insertDuration, insertDuration/time.Duration(b.N), root)
}

// Method 1: Batch write and commit
func TestMethod1(t *testing.T) {
	// Create temporary directory
	os.RemoveAll(mptDir)
	os.MkdirAll(mptDir, os.ModePerm)

	// Create database
	ldb, err := leveldb.New(mptDir, 128, 128, "sliding-test", false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer ldb.Close()

	// Create Trie database

	mdb := ethdb.WrapWithStats(ldb)
	diskDB := rawdb.NewDatabase(mdb)
	// Use PathScheme for pruning support
	cacheConfig := core.DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	trieDB := triedb.NewDatabase(diskDB, cacheConfig.TriedbConfig(false))

	// Load the last root hash
	lastRoot, err := loadLastRoot(diskDB)
	if err != nil {
		t.Fatalf("Failed to load last root hash: %v", err)
	}
	if lastRoot == (common.Hash{}) {
		lastRoot = types.EmptyRootHash
	}

	// Create Trie
	tr, err := trie.New(trie.TrieID(lastRoot), trieDB)
	if err != nil {
		t.Fatalf("Failed to create Trie: %v", err)
	}

	var finalRoot common.Hash = lastRoot
	totalStart := time.Now()

	collector := NewMetricsCollector(100000, mptDir)

	// Batch write data
	for i := 0; i < method1TotalData; i += method1BatchSize {
		batchSize := method1BatchSize
		if i+method1BatchSize > method1TotalData {
			batchSize = method1TotalData - i
		}

		// Write data
		for j := 0; j < batchSize; j++ {
			key, value := generateIndexData(i + j)
			tr.Update(key, value)
		}
		collector.AddInjected(batchSize)

		// Commit and get root hash
		rootStart := time.Now()
		root, nodes := tr.Commit(false)
		collector.AddRootTime(time.Since(rootStart))
		finalRoot = root

		// Update database
		if err := trieDB.Update(root, lastRoot, uint64(i/method1BatchSize), trienode.NewWithNodeSet(nodes), triedb.NewStateSet()); err != nil {
			t.Fatalf("Failed to update database: %v", err)
		}
		if err := trieDB.Commit(root, false); err != nil {
			t.Fatalf("Failed to commit database: %v", err)
		}

		// Prune historical state to keep only current data
		trieDB.Cap(0)

		// Save the last root hash
		if err := saveLastRoot(diskDB, root); err != nil {
			t.Fatalf("Failed to save last root hash: %v", err)
		}

		lastRoot = root

		// Create new Trie to continue writing
		tr, err = trie.New(trie.TrieID(root), trieDB)
		if err != nil {
			t.Fatalf("Failed to create new Trie: %v", err)
		}

		if collector.ShouldReport() {
			t.Logf("Period Summary (Total Items: %d), metrics: %s", collector.totalInjected, collector.GetMetricsString())
			collector.ResetWindow()
		}
	}

	totalTime := time.Since(totalStart)
	t.Logf("Method 1 test completed, total time: %v, final root hash: %x", totalTime, finalRoot)
}

// Method 2: Multiple small batch write tests based on existing root hash
func TestMethod2(t *testing.T) {
	// Create database
	ldb, err := leveldb.New(mptDir, 128, 128, "sliding-test", false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer ldb.Close()

	// Create Trie database
	diskDB := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(diskDB, nil)

	// Load the last root hash
	lastRoot, err := loadLastRoot(diskDB)
	if err != nil {
		t.Fatalf("Failed to load last root hash: %v", err)
	}

	// Create Trie
	tr, err := trie.New(trie.TrieID(lastRoot), trieDB)
	if err != nil {
		t.Fatalf("Failed to create Trie: %v", err)
	}

	// Record time for each operation
	var (
		writeTimes  []time.Duration
		commitTimes []time.Duration
		rootTimes   []time.Duration
	)

	// Prepare CSV data
	csvRecords := [][]string{
		{"Iteration", "Write Time(ns)", "Root Generation Time(ns)", "Commit Time(ns)", "Root Hash"},
	}

	// Perform multiple small batch write tests
	for i := 0; i < method2Iterations; i++ {
		// Write data
		writeStart := time.Now()
		for j := 0; j < method2BatchSize; j++ {
			key, value := generateRandomData()
			tr.Update(key, value)
		}
		writeTime := time.Since(writeStart)
		writeTimes = append(writeTimes, writeTime)

		// Generate root hash
		rootStart := time.Now()
		root, nodes := tr.Commit(false)
		rootTime := time.Since(rootStart)
		rootTimes = append(rootTimes, rootTime)

		// Commit to database
		commitStart := time.Now()
		if err := trieDB.Update(root, common.Hash{}, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			t.Fatalf("Failed to update database: %v", err)
		}
		if err := trieDB.Commit(root, false); err != nil {
			t.Fatalf("Failed to commit database: %v", err)
		}

		// Save the last root hash
		if err := saveLastRoot(diskDB, root); err != nil {
			t.Fatalf("Failed to save last root hash: %v", err)
		}

		commitTime := time.Since(commitStart)
		commitTimes = append(commitTimes, commitTime)

		// Add data to CSV records
		csvRecords = append(csvRecords, []string{
			strconv.Itoa(i + 1),
			strconv.FormatInt(writeTime.Nanoseconds(), 10),
			strconv.FormatInt(rootTime.Nanoseconds(), 10),
			strconv.FormatInt(commitTime.Nanoseconds(), 10),
			root.Hex(),
		})

		// Create new Trie to continue writing
		tr, err = trie.New(trie.TrieID(root), trieDB)
		if err != nil {
			t.Fatalf("Failed to create new Trie: %v", err)
		}

		t.Logf("Iteration %d completed, write time: %v, root generation time: %v, commit time: %v, root hash: %x",
			i+1, writeTime, rootTime, commitTime, root)
	}

	// Calculate average time
	var avgWrite, avgRoot, avgCommit time.Duration
	for i := 0; i < method2Iterations; i++ {
		avgWrite += writeTimes[i]
		avgRoot += rootTimes[i]
		avgCommit += commitTimes[i]
	}
	avgWrite /= time.Duration(method2Iterations)
	avgRoot /= time.Duration(method2Iterations)
	avgCommit /= time.Duration(method2Iterations)

	t.Logf("Method 2 test completed, average time - write: %v, root generation: %v, commit: %v",
		avgWrite, avgRoot, avgCommit)

	// Write results to CSV file
	csvFileName := fmt.Sprintf("mpt_method2_batch%d_iter%d.csv", method2BatchSize, method2Iterations)
	writeCSVFile(t, csvFileName, csvRecords)
}

// TestMPTProof tests the proof functionality of MPT tree
func TestMPTProof(t *testing.T) {
	// Create in-memory database
	memDB := rawdb.NewMemoryDatabase()
	trieDB := triedb.NewDatabase(memDB, nil)

	// Create new MPT tree
	tr, err := trie.New(trie.TrieID(common.Hash{}), trieDB)
	if err != nil {
		t.Fatalf("Failed to create new Trie: %v", err)
	}

	// Insert some test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	t.Log("Starting to insert data into MPT tree")
	for k, v := range testData {
		tr.Update([]byte(k), []byte(v))
	}

	root := tr.Hash()
	//// Commit to get root hash
	//root, _ := tr.Commit(false)
	//if err := trieDB.Commit(root, false); err != nil {
	//	t.Fatalf("Failed to commit tree: %v", err)
	//}
	//t.Logf("MPT tree root hash: %x", root)

	// Test proof for existing key
	proofDB := trienode.NewProofSet()
	key := "key3"

	// Generate Merkle proof for specific key
	t.Logf("Generating proof for key '%s'", key)
	if err := tr.Prove([]byte(key), proofDB); err != nil {
		t.Fatalf("Failed to generate proof: %v", err)
	}

	// Verify proof
	value, err := trie.VerifyProof(root, []byte(key), proofDB)
	if err != nil {
		t.Fatalf("Failed to verify proof: %v", err)
	}

	if string(value) != testData[key] {
		t.Fatalf("Verified value does not match, expected %s, got %s", testData[key], string(value))
	}
	t.Logf("Successfully verified proof for key '%s', value: %s, size: %v.", key, string(value), proofDB.DataSize())

	// Test proof for non-existing key
	nonExistingKey := "non-existing-key"
	proofDB = trienode.NewProofSet()

	t.Logf("Generating proof for non-existing key '%s'", nonExistingKey)
	if err := tr.Prove([]byte(nonExistingKey), proofDB); err != nil {
		t.Fatalf("Failed to generate proof for non-existing key: %v", err)
	}

	// Verify proof for non-existing key
	value, err = trie.VerifyProof(root, []byte(nonExistingKey), proofDB)
	if err != nil {
		t.Fatalf("Failed to verify proof for non-existing key: %v", err)
	}

	if value != nil {
		t.Fatalf("Non-existing key should return nil value, but got: %s", string(value))
	}
	t.Logf("Successfully verified that key '%s' does not exist in the tree", nonExistingKey)
}

// TestLargeMPTProof tests the proof functionality of large-scale MPT tree
func TestLargeMPTProof(t *testing.T) {
	// Test data volume
	const (
		totalItems = 10000000 // 10 million data
	)

	// Define a series of progressively increasing proof scales
	proofSizes := []int{
		2500,   // 2.5k
		5000,   // 5k
		10000,  // 10k
		20000,  // 20k
		40000,  // 40k
		80000,  // 80k
		160000, // 160k
	}

	// Create in-memory database
	memDB := rawdb.NewMemoryDatabase()
	trieDB := triedb.NewDatabase(memDB, nil)

	// Create new MPT tree
	tr, err := trie.New(trie.TrieID(common.Hash{}), trieDB)
	if err != nil {
		t.Fatalf("Failed to create new Trie: %v", err)
	}

	// Generate and insert 10 million random data
	t.Log("Starting to insert 10 million random data into MPT tree...")
	start := time.Now()

	// Save all keys for subsequent proof generation
	allKeys := make([][]byte, totalItems)

	batchSize := 500000 // 500k data per batch
	for i := 0; i < totalItems; i++ {
		key, value := generateRandomData()
		tr.Update(key, value)
		allKeys[i] = key

		// Print progress every batch
		if (i+1)%batchSize == 0 {
			elapsed := time.Since(start)
			t.Logf("Inserted %d items (%.2f%%), time taken: %v", i+1, float64(i+1)*100/float64(totalItems), elapsed)
		}
	}

	insertTime := time.Since(start)
	t.Logf("Inserted %d items completed, total time: %v", totalItems, insertTime)

	// Calculate tree hash
	hashStart := time.Now()
	root := tr.Hash()
	hashTime := time.Since(hashStart)
	t.Logf("MPT tree root hash calculation completed, time taken: %v, root hash: %x", hashTime, root)

	// Randomly select different numbers of keys for proof
	t.Log("Starting to generate proofs of different quantities...")

	// Shuffle algorithm, randomly shuffle all keys
	rand.Shuffle(len(allKeys), func(i, j int) {
		allKeys[i], allKeys[j] = allKeys[j], allKeys[i]
	})

	// Data structure to store proof result data for each scale
	type ProofResult struct {
		proofCount  int           // Number of proofs
		elapsed     time.Duration // Total time
		dataSize    uint64        // Data size
		avgTime     time.Duration // Average time per proof
		bytesPerKey float64       // Average bytes per proof
		keyCount    uint64
	}

	results := make([]ProofResult, len(proofSizes))

	// Test different numbers of proofs
	for i, size := range proofSizes {
		result := generateAndMeasureProofs(t, tr, root, allKeys[:size], size)
		results[i] = result
	}

	// Print summary table
	t.Log("\nProof Scale Performance Comparison:")
	t.Log("------------------------------------------------------------------------------------------------------------------------------")
	t.Log("  Proof Count |   Total Time   |   Data Size   |   Node Count   |  Avg Time/Proof |  Avg Bytes/Proof |  Count Growth   |  Size Growth    ")
	t.Log("------------------------------------------------------------------------------------------------------------------------------")

	// Print first row
	firstResult := results[0]
	t.Logf("  %-10d |  %-10v |  %-10v |  %-10v |  %-14v |  %-14.2f |       -       |       -      ",
		firstResult.proofCount, firstResult.elapsed, bytesToReadable(firstResult.dataSize), firstResult.keyCount,
		firstResult.avgTime, firstResult.bytesPerKey)
	// Print remaining rows and calculate growth ratios
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

// TestHugeMPTProof tests based on database data
func TestHugeMPTProof(t *testing.T) {
	// Create database
	ldb, err := leveldb.New(mptDir, 128, 128, "sliding-test", false)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer ldb.Close()

	root := common.HexToHash("0x8411668e6c14ecf88e2a79c5eaf478fd87829b0a6df3f9c7de6d02081dc57398")

	// Create Trie database
	diskDB := rawdb.NewDatabase(ldb)
	trieDB := triedb.NewDatabase(diskDB, nil)

	// Create new MPT tree
	tr, err := trie.New(trie.TrieID(root), trieDB)
	if err != nil {
		t.Fatalf("Failed to create new Trie: %v", err)
	}

	proofSizes := []int{12000, 20000, 40000, 80000, 160000, 240000, 320000}

	// Save all keys for subsequent proof generation
	allKeys := make([][]byte, 10000000)
	for i := 0; i < 10000000; i++ {
		allKeys[i], _ = generateIndexData(i)
	}

	// Shuffle algorithm, randomly shuffle all keys
	rand.Shuffle(len(allKeys), func(i, j int) {
		allKeys[i], allKeys[j] = allKeys[j], allKeys[i]
	})

	// Data structure to store proof result data for each scale
	type ProofResult struct {
		proofCount  int           // Number of proofs
		elapsed     time.Duration // Total time
		dataSize    uint64        // Data size
		avgTime     time.Duration // Average time per proof
		bytesPerKey float64       // Average bytes per proof
		keyCount    uint64
	}

	results := make([]ProofResult, len(proofSizes))

	// Test different numbers of proofs
	for i, size := range proofSizes {
		result := generateAndMeasureProofs(t, tr, root, allKeys[:size], size)
		results[i] = result
	}

	// Print summary table
	t.Log("\nProof Scale Performance Comparison:")
	t.Log("------------------------------------------------------------------------------------------------------------------------------")
	t.Log("  Proof Count |   Total Time   |   Data Size   |   Node Count   |  Avg Time/Proof |  Avg Bytes/Proof |  Count Growth   |  Size Growth    ")
	t.Log("------------------------------------------------------------------------------------------------------------------------------")

	// Print first row
	firstResult := results[0]
	t.Logf("  %-10d |  %-10v |  %-10v |  %-10v |  %-14v |  %-14.2f |       -       |       -      ",
		firstResult.proofCount, firstResult.elapsed, bytesToReadable(firstResult.dataSize), firstResult.keyCount,
		firstResult.avgTime, firstResult.bytesPerKey)
	// Print remaining rows and calculate growth ratios
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

// generateAndMeasureProofs generates proofs of specific quantity and measures performance
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
			t.Fatalf("Failed to generate proof: %v", err)
		}

		// Print progress every 10k proofs
		if (i+1)%10000 == 0 {
			t.Logf("Generated %d/%d proofs...", i+1, len(keys))
		}
	}
	elapsed := time.Since(start)

	// Verify a random proof
	randomIndex := rand.Intn(len(keys))
	randomKey := keys[randomIndex]
	value, err := trie.VerifyProof(root, randomKey, proofDB)
	if err != nil {
		t.Fatalf("Failed to verify proof: %v", err)
	}

	keyCount := uint64(proofDB.KeyCount())
	dataSize := uint64(proofDB.DataSize())
	avgTime := elapsed / time.Duration(len(keys))
	bytesPerKey := float64(dataSize) / float64(len(keys))

	sizeText := formatSize(size)
	t.Logf("Generated %s proofs completed, total time: %v, average per proof: %v, total proof data size: %v (%.2f bytes/proof)",
		sizeText, elapsed, avgTime, bytesToReadable(dataSize), bytesPerKey)
	t.Logf("Random verification of proof %d successful, key length: %d, value length: %d",
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
