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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

type stemCountingHasher struct {
	inner Hasher
	calls atomic.Int64
}

func (h *stemCountingHasher) Hash(data []byte) []byte {
	h.calls.Add(1)
	return h.inner.Hash(data)
}

func stemTestKey(fill, suffix byte) []byte {
	key := bytes.Repeat([]byte{fill}, StemKeySize)
	key[StemSize] = suffix
	return key
}

func newStemTestTrie(t testing.TB, pruning bool) (*StemTrie, *MemoryDBAdapter) {
	t.Helper()
	db := NewMemoryDBAdapter()
	config := DefaultConfig()
	config.ShardDepth = 0
	backend := NewTrie(nil, db, NewPooledKeccakHasher(), config, pruning)
	trie, err := NewStemTrie(backend)
	if err != nil {
		t.Fatalf("new stem trie: %v", err)
	}
	return trie, db
}

func TestStemBinaryProofsAndEncoding(t *testing.T) {
	hasher := NewPooledKeccakHasher()
	stem := NewStem()
	stem.Put(0, []byte("zero"))
	stem.Put(1, []byte("one"))
	stem.Put(127, []byte("middle"))
	stem.Put(255, []byte("last"))

	root := stem.ValuesRoot(hasher)
	for _, suffix := range []byte{0, 1, 127, 255, 64} {
		proof := stem.Prove(suffix, hasher)
		if !VerifyStemProof(root, proof, hasher) {
			t.Fatalf("proof for suffix %d did not verify", suffix)
		}
	}

	tampered := stem.Prove(127, hasher)
	tampered.Value[0] ^= 0xff
	if VerifyStemProof(root, tampered, hasher) {
		t.Fatal("tampered proof verified")
	}

	encoded := encodeStem(stem, hasher)
	if count, err := stemEncodedValueCount(encoded); err != nil || count != stem.Len() {
		t.Fatalf("encoded value count: got %d err %v, want %d", count, err, stem.Len())
	}
	decoded, err := decodeStem(encoded, hasher)
	if err != nil {
		t.Fatalf("decode stem: %v", err)
	}
	if decoded.Len() != stem.Len() || !bytes.Equal(decoded.ValuesRoot(hasher), root) {
		t.Fatal("stem changed during encoding round trip")
	}

	encoded[len(stemEncodingMagic)] ^= 0x01
	if _, err := decodeStem(encoded, hasher); !errors.Is(err, ErrInvalidStem) {
		t.Fatalf("corrupt values root was accepted: %v", err)
	}
}

func TestStemTrieGroupsSuffixesIntoOneOuterLeaf(t *testing.T) {
	trie, _ := newStemTestTrie(t, true)
	key1 := stemTestKey(0x31, 1)
	key2 := stemTestKey(0x31, 2)
	key3 := stemTestKey(0x42, 3)

	if err := trie.PutBatch([]KeyValue{
		{Key: key1, Value: []byte("value-1")},
		{Key: key2, Value: []byte("value-2")},
		{Key: key1, Value: []byte("value-1-new")},
		{Key: key3, Value: []byte("value-3")},
	}); err != nil {
		t.Fatalf("put batch: %v", err)
	}

	stats := trie.Backend().Stats()
	if stats.LeafCount != 2 {
		t.Fatalf("outer trie should contain two stems, got %d leaves", stats.LeafCount)
	}
	for _, test := range []struct {
		key  []byte
		want string
	}{
		{key1, "value-1-new"},
		{key2, "value-2"},
		{key3, "value-3"},
	} {
		got, err := trie.Get(test.key)
		if err != nil || string(got) != test.want {
			t.Fatalf("get suffix %d: got %q err %v, want %q", test.key[StemSize], got, err, test.want)
		}
	}

	raw, err := trie.Backend().Get(key1[:StemSize])
	if err != nil {
		t.Fatalf("get raw stem: %v", err)
	}
	stem, err := decodeStem(raw, trie.Backend().Hasher())
	if err != nil || stem.Len() != 2 {
		t.Fatalf("raw stem: len %d err %v", stem.Len(), err)
	}

	if err := trie.Delete(key1); err != nil {
		t.Fatalf("delete first suffix: %v", err)
	}
	if _, err := trie.Get(key1); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("deleted suffix still exists: %v", err)
	}
	if got, err := trie.Get(key2); err != nil || string(got) != "value-2" {
		t.Fatalf("sibling suffix was lost: got %q err %v", got, err)
	}
	if err := trie.Delete(key2); err != nil {
		t.Fatalf("delete final suffix: %v", err)
	}
	if _, err := trie.Backend().Get(key1[:StemSize]); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("empty outer stem was not deleted: %v", err)
	}
}

func TestStemArchiveAndUpdateRestoresWholeStem(t *testing.T) {
	db := NewMemoryDBAdapter()
	config := DefaultConfig()
	config.ShardDepth = 8
	backend := NewTrie(nil, db, NewPooledKeccakHasher(), config, true)
	trie, err := NewStemTrie(backend)
	if err != nil {
		t.Fatal(err)
	}
	key1 := stemTestKey(0x51, 7)
	key2 := stemTestKey(0x51, 200)
	if err := trie.Put(key1, []byte("old-7")); err != nil {
		t.Fatal(err)
	}
	if err := trie.Put(key2, []byte("old-200")); err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	if _, err := trie.Backend().CommitToBatch(batch, false); err != nil {
		t.Fatalf("commit hot stem: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	shardID := trie.Backend().GetShardID(key1[:StemSize])
	trie.Backend().SetGlobalEpoch(0)
	trie.Backend().pruneShardIdx = shardID
	if err := trie.Backend().PruneNextShard(); err != nil {
		t.Fatalf("archive stem: %v", err)
	}
	stats := trie.Backend().Stats()
	if stats.ArchivedDataSize != 1 {
		t.Fatalf("two suffixes must archive as one stem, got %d archived records", stats.ArchivedDataSize)
	}

	// A read sees the cold data but does not change its archived state.
	if got, err := trie.Get(key2); err != nil || string(got) != "old-200" {
		t.Fatalf("read archived suffix: got %q err %v", got, err)
	}
	if got := trie.Backend().Stats().ArchivedDataSize; got != 1 {
		t.Fatalf("cold read unexpectedly activated the stem: archived=%d", got)
	}

	// Updating one suffix restores the outer stem and carries the untouched
	// suffix forward in the new payload and ValuesRoot.
	if err := trie.Put(key1, []byte("new-7")); err != nil {
		t.Fatalf("update archived suffix: %v", err)
	}
	if got := trie.Backend().Stats().ArchivedDataSize; got != 0 {
		t.Fatalf("whole stem was not restored: archived=%d", got)
	}
	if got, err := trie.Get(key1); err != nil || string(got) != "new-7" {
		t.Fatalf("updated suffix: got %q err %v", got, err)
	}
	if got, err := trie.Get(key2); err != nil || string(got) != "old-200" {
		t.Fatalf("untouched suffix: got %q err %v", got, err)
	}

	root, proof, err := trie.Prove(key2)
	if err != nil {
		t.Fatalf("prove sibling suffix: %v", err)
	}
	if !VerifyStemProof(root, proof, trie.Backend().Hasher()) {
		t.Fatal("proof against updated stem root did not verify")
	}

	batch = db.NewBatch()
	committedRoot, err := trie.Backend().CommitToBatch(batch, false)
	if err != nil {
		t.Fatalf("commit restored stem: %v", err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	reloadedBackend := NewTrie(committedRoot, db, NewPooledKeccakHasher(), trie.Backend().Config(), true)
	reloaded, err := NewStemTrie(reloadedBackend)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reloaded.Get(key2); err != nil || string(got) != "old-200" {
		t.Fatalf("reloaded untouched suffix: got %q err %v", got, err)
	}
}

func TestStemTrieRejectsNonTreeKeys(t *testing.T) {
	trie, _ := newStemTestTrie(t, false)
	if err := trie.Put(make([]byte, StemKeySize-1), []byte("value")); !errors.Is(err, ErrInvalidStemKey) {
		t.Fatalf("short key was accepted: %v", err)
	}
}

func TestStemTrieApplyBatchMixedUpdates(t *testing.T) {
	trie, _ := newStemTestTrie(t, false)
	key1 := stemTestKey(0x71, 1)
	key2 := stemTestKey(0x71, 2)
	key3 := stemTestKey(0x71, 3)
	if err := trie.PutBatch([]KeyValue{
		{Key: key1, Value: []byte("one")},
		{Key: key2, Value: []byte("two")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := trie.ApplyBatch([]StemUpdate{
		{Key: key1, Value: []byte("one-new")},
		{Key: key2, Delete: true},
		{Key: key3, Value: []byte("three")},
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := trie.Get(key1); err != nil || string(got) != "one-new" {
		t.Fatalf("updated suffix: got %q err %v", got, err)
	}
	if _, err := trie.Get(key2); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("deleted suffix still exists: %v", err)
	}
	if got, err := trie.Get(key3); err != nil || string(got) != "three" {
		t.Fatalf("inserted suffix: got %q err %v", got, err)
	}
}

func TestStemTrieDeleteBatchWithValuesLoadsEachStemOnce(t *testing.T) {
	trie, _ := newStemTestTrie(t, false)
	keys := [][]byte{
		stemTestKey(0x73, 1),
		stemTestKey(0x73, 2),
		stemTestKey(0x74, 3),
	}
	if err := trie.PutBatch([]KeyValue{
		{Key: keys[0], Value: []byte("one")},
		{Key: keys[1], Value: []byte("two")},
		{Key: keys[2], Value: []byte("three")},
	}); err != nil {
		t.Fatal(err)
	}
	deleted, err := trie.DeleteBatchWithValues(keys[:2])
	if err != nil {
		t.Fatal(err)
	}
	if deleted.StemCount != 1 {
		t.Fatalf("two suffixes from one stem loaded %d stems", deleted.StemCount)
	}
	if string(deleted.Values[0]) != "one" || string(deleted.Values[1]) != "two" {
		t.Fatalf("unexpected deleted values: %q %q", deleted.Values[0], deleted.Values[1])
	}
	for _, key := range keys[:2] {
		if _, err := trie.Get(key); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("deleted suffix still exists: %v", err)
		}
	}
	if got, err := trie.Get(keys[2]); err != nil || string(got) != "three" {
		t.Fatalf("unrelated stem changed: got %q err %v", got, err)
	}
}

func TestStemTrieBatchHashesStemOnce(t *testing.T) {
	makeTrie := func() (*StemTrie, *stemCountingHasher) {
		hasher := &stemCountingHasher{inner: NewPooledKeccakHasher()}
		config := DefaultConfig()
		config.ShardDepth = 0
		backend := NewTrie(nil, NewMemoryDBAdapter(), hasher, config, false)
		stem, err := NewStemTrie(backend)
		if err != nil {
			t.Fatal(err)
		}
		return stem, hasher
	}
	entries := make([]KeyValue, 32)
	for i := range entries {
		entries[i] = KeyValue{Key: stemTestKey(0x72, byte(i)), Value: []byte{byte(i)}}
	}
	sequential, sequentialHasher := makeTrie()
	for _, entry := range entries {
		if err := sequential.Put(entry.Key, entry.Value); err != nil {
			t.Fatal(err)
		}
	}
	batched, batchedHasher := makeTrie()
	if err := batched.PutBatch(entries); err != nil {
		t.Fatal(err)
	}
	if got, wantMax := batchedHasher.calls.Load(), sequentialHasher.calls.Load()/4; got >= wantMax {
		t.Fatalf("batch still rehashed the stem per suffix: batched=%d sequential=%d", got, sequentialHasher.calls.Load())
	}
}

func TestStemTrieConcurrentWritesDoNotLoseSuffixes(t *testing.T) {
	trie, _ := newStemTestTrie(t, false)
	again, err := NewStemTrie(trie.Backend())
	if err != nil || again != trie {
		t.Fatalf("backend did not reuse its stem adapter: same=%t err=%v", again == trie, err)
	}
	const writes = 64
	var wg sync.WaitGroup
	errCh := make(chan error, writes)
	for i := 0; i < writes; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := stemTestKey(0x61, byte(i))
			if err := trie.Put(key, []byte(fmt.Sprintf("value-%d", i))); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent put: %v", err)
	}

	raw, err := trie.Backend().Get(stemTestKey(0x61, 0)[:StemSize])
	if err != nil {
		t.Fatal(err)
	}
	stem, err := decodeStem(raw, trie.Backend().Hasher())
	if err != nil {
		t.Fatal(err)
	}
	if stem.Len() != writes {
		t.Fatalf("lost concurrent suffix updates: got %d want %d", stem.Len(), writes)
	}
}

func BenchmarkStemTrieDeleteBatchWithValues100K(b *testing.B) {
	const slots = 100_000
	keys := make([][]byte, slots)
	entries := make([]KeyValue, slots)
	for i := 0; i < slots; i++ {
		key := make([]byte, StemKeySize)
		binary.BigEndian.PutUint32(key[StemSize-4:StemSize], uint32(i/StemSuffixCount))
		key[StemSize] = byte(i % StemSuffixCount)
		keys[i] = key
		entries[i] = KeyValue{Key: key, Value: []byte{byte(i)}}
	}
	b.ReportMetric(float64((slots+StemSuffixCount-1)/StemSuffixCount), "stems/op")
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		trie, _ := newStemTestTrie(b, false)
		if err := trie.PutBatch(entries); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		deleted, err := trie.DeleteBatchWithValues(keys)
		if err != nil {
			b.Fatal(err)
		}
		if len(deleted.Values) != slots {
			b.Fatalf("deleted %d values", len(deleted.Values))
		}
	}
}
