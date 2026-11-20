package hx

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	native "github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

var compactReadSink []byte

// Changed values force path copies, exposing the extra in-memory AMT fields.
func BenchmarkCompactHotWrite(b *testing.B) {
	const count = 4096
	keys := make([][]byte, count)
	for i := range keys {
		key := make([]byte, 32)
		binary.LittleEndian.PutUint32(key[:4], uint32(i*0x9e3779b9))
		keys[i] = key
	}
	for _, variant := range []string{"native", "hx"} {
		b.Run(variant, func(b *testing.B) {
			db := newTestDB()
			var update func([]byte, []byte) error
			if variant == "native" {
				tr := native.NewEmpty(db)
				update = tr.Update
			} else {
				tr := NewEmpty(db)
				tr.SetEpochPolicy(func([]byte) byte { return 0 })
				update = tr.Update
			}
			for _, key := range keys {
				if err := update(key, []byte{1}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := update(keys[i%count], []byte{byte(i/count + 2)}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCompactStubReplace(b *testing.B) {
	tr := NewEmpty(newTestDB())
	path := []byte{1, 0x0f}
	if _, err := tr.MountStub(nil, &Stub{Path: path}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		repl := &Stub{Path: path}
		repl.ContentHash[0] = byte(i)
		if err := tr.ReplaceStub(nil, path, repl); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompactHotRead(b *testing.B) {
	benchmarkCompactHotRead(b, 4096)
}

func BenchmarkCompactLargeHotRead(b *testing.B) {
	benchmarkCompactHotRead(b, 262144)
}

func benchmarkCompactHotRead(b *testing.B, count int) {
	keys := make([][]byte, count)
	for i := range keys {
		key := make([]byte, 32)
		binary.LittleEndian.PutUint32(key[:4], uint32(i*0x9e3779b9))
		keys[i] = key
	}
	for _, variant := range []string{"native", "hx"} {
		b.Run(variant, func(b *testing.B) {
			db := newTestDB()
			var update func([]byte, []byte) error
			var get func([]byte) ([]byte, error)
			if variant == "native" {
				tr := native.NewEmpty(db)
				update, get = tr.Update, tr.Get
			} else {
				tr := NewEmpty(db)
				tr.SetEpochPolicy(func([]byte) byte { return 0 })
				update, get = tr.Update, tr.Get
			}
			for _, key := range keys {
				if err := update(key, []byte{1}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				value, err := get(keys[i%count])
				if err != nil {
					b.Fatal(err)
				}
				compactReadSink = value
			}
		})
	}
}

// BenchmarkCompactColdRead reopens each tree after one pass so every lookup
// traverses a freshly unresolved root. The node store is an in-memory map;
// this isolates trie resolution and decoding from physical disk latency.
func BenchmarkCompactColdRead(b *testing.B) {
	const count = 4096
	keys := make([][]byte, count)
	for i := range keys {
		var seed [8]byte
		binary.LittleEndian.PutUint64(seed[:], uint64(i))
		key := sha256.Sum256(seed[:])
		keys[i] = key[:]
	}
	for _, variant := range []string{"native", "hx"} {
		b.Run(variant, func(b *testing.B) {
			db := newTestDB()
			var root common.Hash
			if variant == "native" {
				tr := native.NewEmpty(db)
				for _, key := range keys {
					if err := tr.Update(key, key); err != nil {
						b.Fatal(err)
					}
				}
				var set *trienode.NodeSet
				root, set = tr.Commit(false)
				for _, node := range set.Nodes {
					if !node.IsDeleted() {
						db.nodes[node.Hash] = node.Blob
					}
				}
			} else {
				tr := NewEmpty(db)
				tr.SetEpochPolicy(func([]byte) byte { return 0 })
				for _, key := range keys {
					if err := tr.Update(key, key); err != nil {
						b.Fatal(err)
					}
				}
				var set *trienode.NodeSet
				root, set = tr.Commit()
				for _, node := range set.Nodes {
					if !node.IsDeleted() {
						db.nodes[node.Hash] = node.Blob
					}
				}
			}
			var get func([]byte) ([]byte, error)
			open := func() error {
				if variant == "native" {
					tr, err := native.New(native.TrieID(root), db)
					if err != nil {
						return err
					}
					get = tr.Get
				} else {
					tr, err := New(root, common.Hash{}, db)
					if err != nil {
						return err
					}
					get = tr.Get
				}
				return nil
			}
			if err := open(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i > 0 && i%count == 0 {
					if err := open(); err != nil {
						b.Fatal(err)
					}
				}
				value, err := get(keys[i%count])
				if err != nil {
					b.Fatal(err)
				}
				compactReadSink = value
			}
		})
	}
}
