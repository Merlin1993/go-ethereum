package mpt

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
)

// These benchmarks are step S0 of the plan: before spending a multi-day remote
// run on any of this, quantify what each structural change is actually worth.
//
// They deliberately use LevelDB rather than a memory store, because the whole
// question is write-path behaviour — batching, write amplification, sync cost —
// and a memory store has none of it. They follow the driver's shape too: 32-byte
// keys, 32-byte values, and one commit per batch instead of per operation.
//
// The comparison this feeds:
//
//	layer A: per-shard tries + hash backend   -> already measured as B3m2 (r~0.36)
//	layer B: single tree   + hash backend     -> BenchmarkWriteHash / BenchmarkReadHash
//	layer C: single tree   + path backend     -> BenchmarkWritePath / BenchmarkReadPath

const benchBatchSize = 4000

// levelStore adapts the real LevelDB handle to the archive KVStore interface. It
// exists next to testStore because the benchmarks must measure disk behaviour,
// not an in-memory map.
type levelStore struct{ *leveldb.Database }

func (s *levelStore) NewBatch() archivetrie.Batcher { return s.Database.NewBatch() }

func benchStore(b *testing.B) *levelStore {
	b.Helper()
	ldb, err := leveldb.New(b.TempDir(), 512, 256, "mpt-bench", false)
	if err != nil {
		b.Fatalf("open bench database: %v", err)
	}
	b.Cleanup(func() { ldb.Close() })
	return &levelStore{ldb}
}

// benchConfig gives each backend the resources the real experiment gives it.
// Comparing a 64 MiB-cached hash backend against a well-provisioned path backend
// flatters the latter; the first run of this benchmark made exactly that mistake
// and reported a 2.1x win that was partly an artefact of unequal budgets.
func benchConfig(backend string) *Config {
	cfg := &Config{
		ShardDepthBits: 16,
		CuckooBuckets:  32,
		CuckooSlots:    4,
		Backend:        backend,
	}
	switch backend {
	case BackendHash:
		// Matches the production flag -traceStressNodeCacheMB=512.
		cfg.NodeCacheBytes = 512 << 20
	case BackendPath:
		// CleanCacheSize has no ceiling in pathdb's sanitize; 256 MiB puts it on
		// par with the hash backend's budget. WriteBufferSize is capped at
		// 256 MiB by pathdb (maxBufferSize), i.e. this is maximal batching.
		cfg.CleanCacheBytes = 256 << 20
		cfg.WriteBufferBytes = 256 << 20
	}
	return cfg
}

// benchKeys mirrors what the trace generates: 32-byte key/value pairs with no
// locality, which is the worst case for per-node write amplification.
func benchKeys(count int) ([][]byte, [][]byte) {
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := range keys {
		keys[i] = make([]byte, 32)
		values[i] = make([]byte, 32)
		if _, err := rand.Read(keys[i]); err != nil {
			panic(err)
		}
		if _, err := rand.Read(values[i]); err != nil {
			panic(err)
		}
	}
	return keys, values
}

func benchWrites(b *testing.B, backend string) {
	const distinct = 1 << 16
	store := benchStore(b)
	tr, err := New(nil, store, benchConfig(backend))
	if err != nil {
		b.Fatalf("new trie: %v", err)
	}
	keys, values := benchKeys(distinct)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tr.Put(keys[i%distinct], values[i%distinct]); err != nil {
			b.Fatal(err)
		}
		if (i+1)%benchBatchSize != 0 {
			continue
		}
		batch := store.NewBatch()
		if _, err := tr.CommitToBatch(batch, false); err != nil {
			b.Fatal(err)
		}
		if err := batch.Write(); err != nil {
			b.Fatal(err)
		}
		batch.Reset()
	}
	// Include the final flush in the measured run: durability is part of the
	// cost profile, and only the path backend has anything buffered at this point.
	if err := tr.Flush(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkWriteHash(b *testing.B) { benchWrites(b, BackendHash) }
func BenchmarkWritePath(b *testing.B) { benchWrites(b, BackendPath) }

// benchReads measures steady-state reads against an already-populated tree, which
// is what the hot path actually does between commits.
func benchReads(b *testing.B, backend string) {
	const distinct = 1 << 16
	store := benchStore(b)
	tr, err := New(nil, store, benchConfig(backend))
	if err != nil {
		b.Fatalf("new trie: %v", err)
	}
	keys, values := benchKeys(distinct)
	for i := range keys {
		if err := tr.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}
	batch := store.NewBatch()
	if _, err := tr.CommitToBatch(batch, false); err != nil {
		b.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tr.Get(keys[i%distinct]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadHash(b *testing.B) { benchReads(b, BackendHash) }
func BenchmarkReadPath(b *testing.B) { benchReads(b, BackendPath) }

// BenchmarkPruneCycle measures the cost of a full prune rotation: every domain
// visited once. With one-nibble domains that is sixteen visits, each of which
// seals one domain and commits.
func BenchmarkPruneCycle(b *testing.B) {
	const distinct = 1 << 14
	store := benchStore(b)
	// One nibble keeps the rotation short so the benchmark stays usable.
	tr, err := New(nil, store, &Config{ShardDepthBits: 4, CuckooBuckets: 32, CuckooSlots: 4, Backend: BackendPath})
	if err != nil {
		b.Fatalf("new trie: %v", err)
	}
	keys, values := benchKeys(distinct)
	for i := range keys {
		if err := tr.Put(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}
	batch := store.NewBatch()
	if _, err := tr.CommitToBatch(batch, false); err != nil {
		b.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for round := 0; round < b.N; round++ {
		for domain := 0; domain < 16; domain++ {
			if err := tr.PruneNextShard(); err != nil {
				b.Fatal(err)
			}
		}
		flush := store.NewBatch()
		if _, err := tr.CommitToBatch(flush, false); err != nil {
			b.Fatal(err)
		}
		if err := flush.Write(); err != nil {
			b.Fatal(err)
		}
	}
	if err := tr.Flush(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkReportedStagedBytes prints how many bytes each backend stages per op,
// which is the write-amplification number the storage claims depend on. It is a
// report rather than an assertion because the healthy value is a design range,
// not a fixed target.
func BenchmarkReportedStagedBytes(b *testing.B) {
	for _, backend := range []string{BackendHash, BackendPath} {
		store := benchStore(b)
		tr, err := New(nil, store, benchConfig(backend))
		if err != nil {
			b.Fatalf("new trie: %v", err)
		}
		batch := &countingBatch{Batcher: store.NewBatch()}
		keys, values := benchKeys(1 << 14)
		for i := range keys {
			if err := tr.Put(keys[i], values[i]); err != nil {
				b.Fatal(err)
			}
			if (i+1)%benchBatchSize != 0 {
				continue
			}
			if _, err := tr.CommitToBatch(batch, false); err != nil {
				b.Fatal(err)
			}
			if err := batch.Write(); err != nil {
				b.Fatal(err)
			}
			// Reset keeps the underlying batch reusable while our own counters
			// accumulate across the whole run.
			batch.Reset()
		}
		ops := float64(len(keys))
		fmt.Printf("backend=%s staged_bytes_per_op=%.1f writes=%d\n",
			backend, float64(batch.bytes)/ops, batch.writes)
	}
}

// countingBatch tallies every staged byte so write amplification is visible
// without reaching into the diagnostic singletons.
type countingBatch struct {
	archivetrie.Batcher
	bytes  int
	writes int
}

func (c *countingBatch) Put(key, value []byte) error {
	c.bytes += len(key) + len(value)
	c.writes++
	return c.Batcher.Put(key, value)
}
