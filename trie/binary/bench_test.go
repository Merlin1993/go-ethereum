package binary

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

func BenchmarkNewTrie(b *testing.B) {
	db := memorydb.New()
	adapter := &binaryDBAdapterForTest{db: db}
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewTrie(nil, adapter, hasher, config, false)
	}
}

func BenchmarkCommit(b *testing.B) {
	db := memorydb.New()
	adapter := &binaryDBAdapterForTest{db: db}
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

type binaryDBAdapterForTest struct {
	db *memorydb.Database
}

func (a *binaryDBAdapterForTest) Put(key, value []byte) error    { return a.db.Put(key, value) }
func (a *binaryDBAdapterForTest) Get(key []byte) ([]byte, error) { return a.db.Get(key) }
func (a *binaryDBAdapterForTest) Delete(key []byte) error        { return a.db.Delete(key) }
func (a *binaryDBAdapterForTest) NewBatch() Batcher {
	return &binaryBatchAdapterForTest{a.db.NewBatch()}
}
func (a *binaryDBAdapterForTest) PutBucket(hash, data []byte) error     { return a.db.Put(hash, data) }
func (a *binaryDBAdapterForTest) GetBucket(hash []byte) ([]byte, error) { return a.db.Get(hash) }
func (a *binaryDBAdapterForTest) DeleteBucket(hash []byte) error        { return a.db.Delete(hash) }

type binaryBatchAdapterForTest struct {
	ethdb.Batch
}

func (a *binaryBatchAdapterForTest) Put(key, value []byte) error { return a.Batch.Put(key, value) }
func (a *binaryBatchAdapterForTest) Delete(key []byte) error     { return a.Batch.Delete(key) }
func (a *binaryBatchAdapterForTest) Write() error                { return a.Batch.Write() }
func (a *binaryBatchAdapterForTest) Reset()                      { a.Batch.Reset() }
func (a *binaryBatchAdapterForTest) ValueSize() int              { return a.Batch.ValueSize() }

type testHasher struct{}

func (h *testHasher) Hash(data []byte) []byte { return data } // simplified
