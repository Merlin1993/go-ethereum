package hx

import (
	"encoding/binary"
	"testing"

	native "github.com/ethereum/go-ethereum/trie"
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
