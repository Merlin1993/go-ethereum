package archive

import (
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/leveldb"
)

func benchmarkArchiveWriteCycle(b *testing.B, stateDB KVStore, archiveDB ArchiveStore, archiveItemCacheLimit int) {
	const (
		poolSize   = 1 << 15
		batchSize  = 1000
		valueSize  = 32
		keySize    = 32
		shardDepth = 8
	)

	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ShardDepth = shardDepth
	config.ArchiveItemCacheLimit = archiveItemCacheLimit
	config.ArchiveDB = archiveDB
	trie := NewTrie(nil, stateDB, hasher, config, true)

	rng := rand.New(rand.NewSource(1))
	keyPool := make([][]byte, poolSize)
	for i := range keyPool {
		keyPool[i] = make([]byte, keySize)
		rng.Read(keyPool[i])
		val := make([]byte, valueSize)
		rng.Read(val)
		if err := trie.Put(keyPool[i], val); err != nil {
			b.Fatalf("seed put failed: %v", err)
		}
	}
	if _, err := trie.Commit(); err != nil {
		b.Fatalf("seed commit failed: %v", err)
	}

	nextReplace := 0
	makeValue := func() []byte {
		val := make([]byte, valueSize)
		rng.Read(val)
		return val
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := trie.PruneNextShard(); err != nil {
			b.Fatalf("prune failed: %v", err)
		}
		if err := trie.FlushArchives(); err != nil {
			b.Fatalf("flush failed: %v", err)
		}

		for j := 0; j < batchSize; j++ {
			key := make([]byte, keySize)
			rng.Read(key)
			if err := trie.Put(key, makeValue()); err != nil {
				b.Fatalf("insert failed: %v", err)
			}
			keyPool[nextReplace] = key
			nextReplace = (nextReplace + 1) % len(keyPool)
		}
		for j := 0; j < batchSize; j++ {
			key := keyPool[rng.Intn(len(keyPool))]
			if err := trie.Put(key, makeValue()); err != nil {
				b.Fatalf("update failed: %v", err)
			}
		}

		batch := trie.db.NewBatch()
		if _, err := trie.CommitToBatch(batch, false); err != nil {
			b.Fatalf("commit failed: %v", err)
		}
		if err := batch.Write(); err != nil {
			b.Fatalf("batch write failed: %v", err)
		}
		batch.Reset()
	}
}

func BenchmarkArchiveWriteCycleMemory(b *testing.B) {
	db := NewMemoryDBAdapter()
	benchmarkArchiveWriteCycle(b, db, db, 0)
}

func BenchmarkArchiveWriteCycleMemoryCached(b *testing.B) {
	db := NewMemoryDBAdapter()
	benchmarkArchiveWriteCycle(b, db, db, -1)
}

func BenchmarkArchiveWriteCycleLevelDB(b *testing.B) {
	baseDir := b.TempDir()

	stateDB, err := leveldb.New(filepath.Join(baseDir, "state"), 512, 256, "state", false)
	if err != nil {
		b.Fatalf("open state db failed: %v", err)
	}
	defer stateDB.Close()

	archiveDB, err := leveldb.New(filepath.Join(baseDir, "archive"), 512, 256, "archive", false)
	if err != nil {
		b.Fatalf("open archive db failed: %v", err)
	}
	defer archiveDB.Close()

	benchmarkArchiveWriteCycle(b, &LevelDBAdapter{stateDB}, &LevelDBAdapter{archiveDB}, 0)
}
