package archive

import (
	"math/rand"
	"testing"
)

func benchmarkArchiveWriteCycle(b *testing.B, stateDB KVStore) {
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
	benchmarkArchiveWriteCycle(b, db)
}
