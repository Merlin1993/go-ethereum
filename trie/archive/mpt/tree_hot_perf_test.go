package mpt

import (
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

var treeHotReadSink []byte

// BenchmarkTreeHotRead separates hx lookup from the AMT wrapper's locking,
// handle check, and returned-value copy on a resident hot tree.
func BenchmarkTreeHotRead(b *testing.B) {
	const count = 4096
	store := &testStore{memorydb.New()}
	tr, err := New(nil, store, &Config{ShardDepthBits: 4, BucketCapacity: 100, CuckooBuckets: 64, CuckooSlots: 4, Backend: BackendPath})
	if err != nil {
		b.Fatal(err)
	}
	keys := make([][]byte, count)
	for i := range keys {
		key := make([]byte, 32)
		binary.LittleEndian.PutUint32(key, uint32(i*0x9e3779b9))
		keys[i] = key
		if err := tr.Put(key, make([]byte, 32)); err != nil {
			b.Fatal(err)
		}
	}
	for _, variant := range []string{"hx", "locked", "wrapper"} {
		b.Run(variant, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				key := keys[i%count]
				var value []byte
				var err error
				switch variant {
				case "hx":
					value, err = tr.hot.Get(key)
				case "locked":
					tr.mu.Lock()
					value, err = tr.hot.Get(key)
					tr.mu.Unlock()
				case "wrapper":
					value, err = tr.Get(key)
				}
				if err != nil {
					b.Fatal(err)
				}
				treeHotReadSink = value
			}
		})
	}
}
