package archive

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func BenchmarkNewTrie(b *testing.B) {
	db := memorydb.New()
	adapter := &archiveDBAdapterForTest{db: db}
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewTrie(nil, adapter, hasher, config, false)
	}
}

func BenchmarkCommit(b *testing.B) {
	db := memorydb.New()
	adapter := &archiveDBAdapterForTest{db: db}
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	t := NewTrie(nil, adapter, hasher, config, false)

	// Add some data to make it dirty
	t.Put([]byte("test"), []byte("value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.Commit()
		// Re-mark dirty for subsequent iterations if needed,
		// but even with empty commit it should be fast now.
	}
}

type archiveDBAdapterForTest struct {
	db *memorydb.Database
}

func (a *archiveDBAdapterForTest) Put(key, value []byte) error    { return a.db.Put(key, value) }
func (a *archiveDBAdapterForTest) Get(key []byte) ([]byte, error) { return a.db.Get(key) }
func (a *archiveDBAdapterForTest) Delete(key []byte) error        { return a.db.Delete(key) }
func (a *archiveDBAdapterForTest) NewBatch() Batcher {
	return &archiveBatchAdapterForTest{a.db.NewBatch()}
}
func (a *archiveDBAdapterForTest) PutBucket(hash, data []byte) error     { return a.db.Put(hash, data) }
func (a *archiveDBAdapterForTest) GetBucket(hash []byte) ([]byte, error) { return a.db.Get(hash) }
func (a *archiveDBAdapterForTest) DeleteBucket(hash []byte) error        { return a.db.Delete(hash) }

type archiveBatchAdapterForTest struct {
	ethdb.Batch
}

func (a *archiveBatchAdapterForTest) Put(key, value []byte) error { return a.Batch.Put(key, value) }
func (a *archiveBatchAdapterForTest) Delete(key []byte) error     { return a.Batch.Delete(key) }
func (a *archiveBatchAdapterForTest) Write() error                { return a.Batch.Write() }
func (a *archiveBatchAdapterForTest) Reset()                      { a.Batch.Reset() }
func (a *archiveBatchAdapterForTest) ValueSize() int              { return a.Batch.ValueSize() }
