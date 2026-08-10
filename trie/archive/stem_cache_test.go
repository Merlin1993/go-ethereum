// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package archive

import (
	"encoding/binary"
	"testing"
)

func TestStemStateCacheCloneIsolation(t *testing.T) {
	cache := newStemStateCache(2, -1)
	stem := NewStem()
	stem.Put(1, []byte("one"))
	stem.ValuesRoot(NewPooledKeccakHasher())
	key := make([]byte, StemSize)
	key[0] = 0x42
	cache.add(key, stem, true)

	first, split, ok := cache.get(key)
	if !ok || !split {
		t.Fatal("cached stem not found")
	}
	first.Put(1, []byte("changed"))
	second, _, ok := cache.get(key)
	if !ok {
		t.Fatal("cached stem disappeared")
	}
	if got, _ := second.Get(1); string(got) != "one" {
		t.Fatalf("cache entry was mutated through returned clone: %q", got)
	}
}

func TestStemStateCacheLimits(t *testing.T) {
	stem := NewStem()
	stem.Put(1, []byte("value"))
	stem.ValuesRoot(NewPooledKeccakHasher())

	entryLimited := newStemStateCache(1, -1)
	entryLimited.add([]byte("first"), stem, true)
	entryLimited.add([]byte("second"), stem, true)
	if diag := entryLimited.diagnostics(); diag.Entries != 1 || diag.EntryEvictions != 1 {
		t.Fatalf("entry limit not enforced: %+v", diag)
	}

	byteLimited := newStemStateCache(2, 1)
	byteLimited.add([]byte("oversized"), stem, true)
	if diag := byteLimited.diagnostics(); diag.Entries != 0 || diag.OversizedRejects != 1 {
		t.Fatalf("byte limit not enforced: %+v", diag)
	}
}

func BenchmarkStemCacheRepeatedUpdates(b *testing.B) {
	for _, test := range []struct {
		name       string
		cacheLimit int
	}{
		{name: "disabled", cacheLimit: -1},
		{name: "enabled", cacheLimit: 128},
	} {
		b.Run(test.name, func(b *testing.B) {
			db := NewMemoryDBAdapter()
			config := DefaultConfig()
			config.ShardDepth = 8
			config.StemCacheLimit = test.cacheLimit
			config.StemCacheBytesLimit = 4 * 1024 * 1024
			backend := NewTrie(nil, db, NewPooledKeccakHasher(), config, false)
			trie, err := NewStemTrie(backend)
			if err != nil {
				b.Fatal(err)
			}
			key := stemTestKey(0x51, 1)
			if err := trie.Put(stemTestKey(0x51, 2), []byte("sibling")); err != nil {
				b.Fatal(err)
			}
			value := make([]byte, 8)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				binary.LittleEndian.PutUint64(value, uint64(i))
				if err := trie.Put(key, value); err != nil {
					b.Fatal(err)
				}
				batch := db.NewBatch()
				if _, err := backend.CommitToBatch(batch, true); err != nil {
					b.Fatal(err)
				}
				if err := batch.Write(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
