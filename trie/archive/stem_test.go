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
	"time"
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

func TestLatencyHistogramPercentiles(t *testing.T) {
	var previous, current LatencyHistogram
	for _, nanos := range []int64{100, 200, 300, 400, 10_000} {
		current.Buckets[latencyBucketIndex(nanos)]++
		current.Count++
	}
	previous.Buckets[latencyBucketIndex(100)]++
	previous.Count++
	window := current.Sub(previous)
	if window.Count != 4 {
		t.Fatalf("window count: got %d want 4", window.Count)
	}
	if p50, p95 := window.Percentile(50), window.Percentile(95); p50 < 200*time.Nanosecond || p50 > 400*time.Nanosecond || p95 < 10*time.Microsecond {
		t.Fatalf("unexpected histogram percentiles: p50=%v p95=%v", p50, p95)
	}
}

func TestStemIncrementalCommitmentMatchesFullTree(t *testing.T) {
	hasher := NewPooledKeccakHasher()
	stem := NewStem()
	checkRoot := func(step string) {
		t.Helper()
		got := stem.ValuesRoot(hasher)
		want := fullStemValuesRoot(stem, hasher)
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: incremental root %x, full root %x", step, got, want)
		}
	}
	checkRoot("empty")
	for i := 0; i < StemSuffixCount; i++ {
		stem.Put(byte(i), bytes.Repeat([]byte{byte(i), byte(255 - i)}, i%7+1))
		checkRoot(fmt.Sprintf("put-%d", i))
	}
	for i := StemSuffixCount - 1; i >= 0; i -= 3 {
		stem.Put(byte(i), []byte{0xff, byte(i)})
		checkRoot(fmt.Sprintf("update-%d", i))
	}
	for i := 0; i < StemSuffixCount; i += 2 {
		stem.Delete(byte(i))
		checkRoot(fmt.Sprintf("delete-%d", i))
	}
	for i := 0; i < StemSuffixCount; i++ {
		proof := stem.Prove(byte(i), hasher)
		if !VerifyStemProof(stem.ValuesRoot(hasher), proof, hasher) {
			t.Fatalf("proof for suffix %d did not verify after incremental updates", i)
		}
	}
}

func TestStemCommitmentCachesEightLevelPath(t *testing.T) {
	hasher := &stemCountingHasher{inner: NewPooledKeccakHasher()}
	stem := NewStem()
	stem.Put(17, []byte("first"))
	stem.ValuesRoot(hasher)
	initial := hasher.calls.Load()
	if want := int64(1 + StemProofDepth + 1 + StemProofDepth); initial != want {
		t.Fatalf("initial sparse tree used %d hashes, want %d", initial, want)
	}
	stem.ValuesRoot(hasher)
	if got := hasher.calls.Load(); got != initial {
		t.Fatalf("unchanged root was rehashed: before=%d after=%d", initial, got)
	}
	stem.Put(17, []byte("second"))
	stem.ValuesRoot(hasher)
	if got, want := hasher.calls.Load()-initial, int64(1+StemProofDepth); got != want {
		t.Fatalf("single suffix update used %d hashes, want %d", got, want)
	}
}

func fullStemValuesRoot(stem *Stem, hasher Hasher) []byte {
	level := make([][]byte, StemSuffixCount)
	empty := hasher.Hash(stemEmptyLeafDomain)
	for i := range level {
		if stem != nil && stem.has(byte(i)) {
			level[i] = stemValueHash(stem.values[byte(i)], hasher)
		} else {
			level[i] = empty
		}
	}
	for len(level) > 1 {
		parents := make([][]byte, len(level)/2)
		for i := range parents {
			parents[i] = stemBranchHash(level[i*2], level[i*2+1], hasher)
		}
		level = parents
	}
	return level[0]
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

	stem, err := trie.loadStem(key1[:StemSize])
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

func TestStemTrieReplaceStemDropsSiblingSuffixes(t *testing.T) {
	trie, _ := newStemTestTrie(t, false)
	key1 := stemTestKey(0x32, 1)
	key2 := stemTestKey(0x32, 2)
	if err := trie.PutBatch([]KeyValue{
		{Key: key1, Value: []byte("old-one")},
		{Key: key2, Value: []byte("old-two")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := trie.ReplaceStem(key1, []byte("new-one")); err != nil {
		t.Fatal(err)
	}
	if got, err := trie.Get(key1); err != nil || string(got) != "new-one" {
		t.Fatalf("replacement value: got %q err %v", got, err)
	}
	if _, err := trie.Get(key2); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("replacement retained sibling suffix: %v", err)
	}
}

func TestStemArchiveAndUpdateRestoresWholeStem(t *testing.T) {
	db := NewMemoryDBAdapter()
	config := DefaultConfig()
	config.ShardDepth = 8
	config.StemMode = true
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

func TestStemStatsStorageBreakdownAndFilterNegatives(t *testing.T) {
	db := NewMemoryDBAdapter()
	config := DefaultConfig()
	config.ShardDepth = 8
	config.StemMode = true
	backend := NewTrie(nil, db, NewPooledKeccakHasher(), config, true)
	trie, err := NewStemTrie(backend)
	if err != nil {
		t.Fatal(err)
	}
	key1 := stemTestKey(0x52, 7)
	key2 := stemTestKey(0x52, 200)
	value1 := []byte("old-7")
	value2 := []byte("old-200")
	if err := trie.PutBatch([]KeyValue{{Key: key1, Value: value1}, {Key: key2, Value: value2}}); err != nil {
		t.Fatal(err)
	}
	commit := func() {
		batch := db.NewBatch()
		if _, err := trie.Backend().CommitToBatch(batch, false); err != nil {
			t.Fatal(err)
		}
		if err := batch.Write(); err != nil {
			t.Fatal(err)
		}
	}
	commit()
	shardID := trie.Backend().GetShardID(key1[:StemSize])
	trie.Backend().SetGlobalEpoch(0)
	trie.Backend().pruneShardIdx = shardID
	if err := trie.Backend().PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	commit()

	stats := trie.Backend().StatsWithDiagnostics(20, 7, true)
	if !stats.StorageBreakdownValid || stats.StorageBreakdownReadFailures != 0 {
		t.Fatalf("invalid storage breakdown: valid=%t failures=%d active=%d archived=%d activeMeta=%d archivedMeta=%d activeSuffix=%d archivedSuffix=%d",
			stats.StorageBreakdownValid, stats.StorageBreakdownReadFailures,
			stats.ActiveLogicalValues, stats.ArchivedLogicalValues,
			stats.ActiveStemMetadataBytes, stats.ArchivedStemMetadataBytes,
			stats.ActiveSuffixValueBytes, stats.ArchivedSuffixValueBytes)
	}
	wantMetadata := int64(len(flatValueDataKey(key1[:StemSize])) + len(stemMetadataMagic) + StemSuffixCount/8)
	wantSuffixes := int64(len(flatValueDataKey(key1)) + len(value1) + len(flatValueDataKey(key2)) + len(value2))
	if stats.ArchivedStemMetadataBytes != wantMetadata || stats.ArchivedSuffixValueBytes != wantSuffixes {
		t.Fatalf("archived flat bytes: metadata=%d/%d suffixes=%d/%d",
			stats.ArchivedStemMetadataBytes, wantMetadata, stats.ArchivedSuffixValueBytes, wantSuffixes)
	}
	if stats.ArchiveBucketLogicalBytes == 0 || stats.ArchivedPayloadLogicalBytes == 0 ||
		stats.ReachableLogicalBytes != stats.ActiveOnlyLogicalBytes+stats.ArchivedPayloadLogicalBytes {
		t.Fatalf("incomplete logical byte totals: %+v", stats)
	}
	filterStats := stats.FilterFPStats()
	if filterStats == nil || filterStats.SampledBuckets != 1 || filterStats.PositiveQueries != 1 ||
		filterStats.TruePositives != 1 || filterStats.NegativeQueries != 20 ||
		filterStats.FilterChecks != 21 || len(filterStats.Groups) != 3 {
		t.Fatalf("unexpected synthetic filter stats: %+v", filterStats)
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

func TestStemTrieApplyBatchReplaceDropsSiblingSuffixes(t *testing.T) {
	trie, db := newStemTestTrie(t, false)
	key1 := stemTestKey(0x72, 1)
	key2 := stemTestKey(0x72, 2)
	if err := trie.PutBatch([]KeyValue{
		{Key: key1, Value: []byte("old-one")},
		{Key: key2, Value: []byte("old-two")},
	}); err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	if _, err := trie.Backend().CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	if err := trie.ApplyBatch([]StemUpdate{{
		Key:     key1,
		Value:   []byte("new-one"),
		Replace: true,
	}}); err != nil {
		t.Fatal(err)
	}
	batch = db.NewBatch()
	if _, err := trie.Backend().CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if got, err := trie.Get(key1); err != nil || string(got) != "new-one" {
		t.Fatalf("replacement value: got %q err %v", got, err)
	}
	if _, err := trie.Get(key2); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("replacement retained sibling suffix: %v", err)
	}
	if _, err := trie.Backend().GetFlatValue(key2); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("replacement retained sibling flat record: %v", err)
	}
}

func TestStemTrieApplyBatchConcurrentPathReloadsInOneShard(t *testing.T) {
	db := NewMemoryDBAdapter()
	config := DefaultConfig()
	config.ShardDepth = 8
	config.NodeStorageScheme = NodeStoragePath
	config.NodeCacheLimit = -1
	config.NodeCacheBytesLimit = -1
	config.NodeCacheWarmPathBits = -2
	backend := NewTrie(nil, db, NewPooledKeccakHasher(), config, false)
	trie, err := NewStemTrie(backend)
	if err != nil {
		t.Fatal(err)
	}

	const stems = 64
	keys := make([][]byte, stems)
	initial := make([]KeyValue, stems)
	for i := range keys {
		key := make([]byte, StemKeySize)
		key[0] = 0x42 // Keep every stem in the same depth-8 shard.
		key[1] = byte(i)
		key[StemSize] = 1
		keys[i] = key
		initial[i] = KeyValue{Key: key, Value: []byte{byte(i)}}
	}
	if err := trie.PutBatch(initial); err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	if _, err := backend.CommitToBatch(batch, true); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	updates := make([]StemUpdate, stems)
	for i, key := range keys {
		updates[i] = StemUpdate{Key: key, Value: []byte{byte(i), 0xff}}
	}
	if err := trie.ApplyBatch(updates); err != nil {
		t.Fatal(err)
	}
	for i, key := range keys {
		got, err := trie.Get(key)
		if err != nil || !bytes.Equal(got, updates[i].Value) {
			t.Fatalf("stem %d after concurrent reload: got %x err %v", i, got, err)
		}
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

	stem, err := trie.loadStem(stemTestKey(0x61, 0)[:StemSize])
	if err != nil {
		t.Fatal(err)
	}
	if stem.Len() != writes {
		t.Fatalf("lost concurrent suffix updates: got %d want %d", stem.Len(), writes)
	}
}

func TestStemTrieSkipsUnchangedPut(t *testing.T) {
	trie, _ := newStemTestTrie(t, false)
	key := stemTestKey(0x62, 9)
	value := []byte("unchanged")
	if err := trie.Put(key, value); err != nil {
		t.Fatal(err)
	}
	before := LastUpdateDiagnostics()
	if err := trie.Put(key, value); err != nil {
		t.Fatal(err)
	}
	window := LastUpdateDiagnostics().Sub(before)
	if window.StemPutCalls != 1 || window.StemPutNoops != 1 {
		t.Fatalf("unchanged put diagnostics: calls=%d noops=%d", window.StemPutCalls, window.StemPutNoops)
	}
	if window.StemPutCommitmentHashes != 9 {
		t.Fatalf("unchanged value rebuilt commitment: hashes=%d", window.StemPutCommitmentHashes)
	}
	if window.ShardPutCalls != 1 {
		t.Fatalf("unchanged archived value would not be refreshed: shard puts=%d", window.ShardPutCalls)
	}
}

func TestStemTrieSplitStorageWritesOnlyChangedRecords(t *testing.T) {
	trie, db := newStemTestTrie(t, false)
	key1 := stemTestKey(0x63, 1)
	key2 := stemTestKey(0x63, 2)
	key3 := stemTestKey(0x63, 3)
	commit := func() {
		t.Helper()
		batch := db.NewBatch()
		if _, err := trie.Backend().CommitToBatch(batch, false); err != nil {
			t.Fatal(err)
		}
		if err := batch.Write(); err != nil {
			t.Fatal(err)
		}
	}
	if err := trie.PutBatch([]KeyValue{
		{Key: key1, Value: []byte("one")},
		{Key: key2, Value: []byte("two")},
	}); err != nil {
		t.Fatal(err)
	}
	commit()

	before := LastArchiveCumulativeDiagnostics()
	updated := []byte("one-updated")
	if err := trie.Put(key1, updated); err != nil {
		t.Fatal(err)
	}
	commit()
	after := LastArchiveCumulativeDiagnostics()
	if puts, bytesWritten := after.FlatValuePuts-before.FlatValuePuts, after.FlatValuePutBytes-before.FlatValuePutBytes; puts != 1 || bytesWritten != int64(len(updated)) {
		t.Fatalf("existing suffix update wrote %d records/%d bytes, want 1/%d", puts, bytesWritten, len(updated))
	}

	before = after
	added := []byte("three")
	if err := trie.Put(key3, added); err != nil {
		t.Fatal(err)
	}
	commit()
	after = LastArchiveCumulativeDiagnostics()
	metadataSize := int64(len(stemMetadataMagic) + StemSuffixCount/8)
	if puts, bytesWritten := after.FlatValuePuts-before.FlatValuePuts, after.FlatValuePutBytes-before.FlatValuePutBytes; puts != 2 || bytesWritten != metadataSize+int64(len(added)) {
		t.Fatalf("new suffix wrote %d records/%d bytes, want 2/%d", puts, bytesWritten, metadataSize+int64(len(added)))
	}

	before = after
	if err := trie.Delete(key3); err != nil {
		t.Fatal(err)
	}
	commit()
	after = LastArchiveCumulativeDiagnostics()
	if puts, deletes, bytesWritten := after.FlatValuePuts-before.FlatValuePuts, after.FlatValueDeletes-before.FlatValueDeletes, after.FlatValuePutBytes-before.FlatValuePutBytes; puts != 1 || deletes != 1 || bytesWritten != metadataSize {
		t.Fatalf("suffix delete wrote %d puts/%d deletes/%d bytes, want 1/1/%d", puts, deletes, bytesWritten, metadataSize)
	}

	stem, err := trie.loadStem(key1[:StemSize])
	if err != nil {
		t.Fatal(err)
	}
	valueRef, _, err := trie.Backend().GetValueRef(key1[:StemSize])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(valueRef, stem.ValuesRoot(trie.Backend().Hasher())) {
		t.Fatal("outer leaf does not store the stem root")
	}

	if _, err := trie.DeleteBatchWithValues([][]byte{key1, key2}); err != nil {
		t.Fatal(err)
	}
	commit()
	for _, physicalKey := range [][]byte{key1[:StemSize], key1, key2, key3} {
		if _, err := trie.Backend().GetFlatValue(physicalKey); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("deleted stem retained flat record %x: %v", physicalKey, err)
		}
	}
}

func TestStemTrieMigratesLegacyBlobOnWrite(t *testing.T) {
	trie, db := newStemTestTrie(t, false)
	key1 := stemTestKey(0x64, 1)
	key2 := stemTestKey(0x64, 2)
	legacy := NewStem()
	legacy.Put(key1[StemSize], []byte("old-one"))
	legacy.Put(key2[StemSize], []byte("old-two"))
	if err := trie.Backend().Put(key1[:StemSize], encodeStem(legacy, trie.Backend().Hasher())); err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	root, err := trie.Backend().CommitToBatch(batch, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	backend := NewTrie(root, db, NewPooledKeccakHasher(), trie.Backend().Config(), false)
	reloaded, err := NewStemTrie(backend)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reloaded.Get(key2); err != nil || string(got) != "old-two" {
		t.Fatalf("legacy read: got %q err %v", got, err)
	}
	if err := reloaded.Put(key1, []byte("new-one")); err != nil {
		t.Fatal(err)
	}
	batch = db.NewBatch()
	root, err = reloaded.Backend().CommitToBatch(batch, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	metadata, err := reloaded.Backend().GetFlatValue(key1[:StemSize])
	if err != nil || len(metadata) < len(stemMetadataMagic) || !bytes.Equal(metadata[:len(stemMetadataMagic)], stemMetadataMagic[:]) {
		t.Fatalf("legacy blob was not migrated: metadata=%x err=%v", metadata, err)
	}

	cleanBackend := NewTrie(root, db, NewPooledKeccakHasher(), trie.Backend().Config(), false)
	clean, err := NewStemTrie(cleanBackend)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := clean.Get(key1); err != nil || string(got) != "new-one" {
		t.Fatalf("migrated update: got %q err %v", got, err)
	}
	if got, err := clean.Get(key2); err != nil || string(got) != "old-two" {
		t.Fatalf("migrated sibling: got %q err %v", got, err)
	}
}

func TestStemTrieReplaceStemStableBitmapWritesOnlyValue(t *testing.T) {
	trie, db := newStemTestTrie(t, false)
	key := stemTestKey(0x65, 0)
	if err := trie.ReplaceStem(key, []byte("first")); err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	if _, err := trie.Backend().CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	before := LastArchiveCumulativeDiagnostics()
	value := []byte("second")
	if err := trie.ReplaceStem(key, value); err != nil {
		t.Fatal(err)
	}
	batch = db.NewBatch()
	if _, err := trie.Backend().CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	after := LastArchiveCumulativeDiagnostics()
	if puts, bytesWritten := after.FlatValuePuts-before.FlatValuePuts, after.FlatValuePutBytes-before.FlatValuePutBytes; puts != 1 || bytesWritten != int64(len(value)) {
		t.Fatalf("stable replacement wrote %d records/%d bytes, want 1/%d", puts, bytesWritten, len(value))
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

var benchmarkStemRoot []byte

func BenchmarkStemSingleSuffixUpdate(b *testing.B) {
	hasher := NewPooledKeccakHasher()
	b.Run("full-rebuild", func(b *testing.B) {
		stem := NewStem()
		stem.Put(17, []byte{0})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stem.Put(17, []byte{byte(i)})
			benchmarkStemRoot = fullStemValuesRoot(stem, hasher)
		}
	})
	b.Run("incremental", func(b *testing.B) {
		stem := NewStem()
		stem.Put(17, []byte{0})
		stem.ValuesRoot(hasher)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			stem.Put(17, []byte{byte(i)})
			benchmarkStemRoot = stem.ValuesRoot(hasher)
		}
	})
}

func BenchmarkStemDecodeUpdateEncode(b *testing.B) {
	hasher := NewPooledKeccakHasher()
	empty := stemEmptyRoots(hasher)
	stem := NewStem()
	stem.Put(17, []byte{2})
	encoded := encodeStem(stem, hasher)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stem, err := decodeStemWithEmpty(encoded, hasher, &empty)
		if err != nil {
			b.Fatal(err)
		}
		stem.Put(17, []byte{byte(i & 1)})
		encoded = encodeStem(stem, hasher)
	}
	benchmarkStemRoot = encoded
}

func BenchmarkStemTriePutExistingAccount(b *testing.B) {
	for _, benchmark := range []struct {
		name    string
		replace bool
	}{
		{name: "load-before-put"},
		{name: "replace", replace: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			trie, _ := newStemTestTrie(b, false)
			key := stemTestKey(0x81, 0)
			if err := trie.Put(key, []byte{2}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if benchmark.replace {
					err = trie.ReplaceStem(key, []byte{byte(i & 1)})
				} else {
					err = trie.Put(key, []byte{byte(i & 1)})
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStemTriePutNewAccount(b *testing.B) {
	for _, benchmark := range []struct {
		name    string
		replace bool
	}{
		{name: "load-before-put"},
		{name: "replace", replace: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			trie, _ := newStemTestTrie(b, false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := make([]byte, StemKeySize)
				binary.BigEndian.PutUint64(key[StemSize-8:StemSize], uint64(i+1))
				var err error
				if benchmark.replace {
					err = trie.ReplaceStem(key, []byte{byte(i)})
				} else {
					err = trie.Put(key, []byte{byte(i)})
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
