// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package archive

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type concurrentHashTestHasher struct {
	inner   Hasher
	enabled atomic.Bool
	active  atomic.Int64
	max     atomic.Int64
}

func (h *concurrentHashTestHasher) Hash(data []byte) []byte {
	if h.enabled.Load() {
		active := h.active.Add(1)
		for {
			old := h.max.Load()
			if active <= old || h.max.CompareAndSwap(old, active) {
				break
			}
		}
		time.Sleep(250 * time.Microsecond)
		h.active.Add(-1)
	}
	return h.inner.Hash(data)
}

func binaryRootForTest(hasher Hasher, roots map[int][]byte, shardDepth, depth, prefix int) []byte {
	if depth == shardDepth {
		return normalizeRootHash(roots[prefix])
	}
	left := binaryRootForTest(hasher, roots, shardDepth, depth+1, prefix<<1)
	right := binaryRootForTest(hasher, roots, shardDepth, depth+1, prefix<<1|1)
	if left == nil && right == nil {
		return nil
	}
	branch := newRootBranch()
	branch.childHashes[0] = left
	branch.childHashes[1] = right
	return hasher.Hash(serializeRootBranch(branch, depth))
}

func TestShardRootsHashAlongBinaryPaths(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ShardDepth = 4
	trie := NewTrie(nil, db, hasher, config, true)

	values := map[int][]byte{1: []byte("one"), 6: []byte("six"), 15: []byte("fifteen")}
	keys := make(map[int][]byte)
	for id, value := range values {
		key := make([]byte, 32)
		key[0] = byte(id << 4)
		keys[id] = key
		if err := trie.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	root, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}
	shardRoots := make(map[int][]byte)
	for id := range values {
		shardRoots[id] = trie.GetShardRoot(id)
	}
	want := binaryRootForTest(hasher, shardRoots, config.ShardDepth, 0, 0)
	if !bytes.Equal(root, want) {
		t.Fatalf("global root is not the direct binary path root: got %x want %x", root, want)
	}
	encoded, err := db.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != rootBranchSize || encoded[0] != RootBranchHeader || encoded[1] != 0 {
		t.Fatalf("unexpected root branch encoding: %x", encoded)
	}
	db.mu.RLock()
	branchDepths := make(map[byte]bool)
	for _, blob := range db.data {
		if len(blob) == rootBranchSize && blob[0] == RootBranchHeader {
			branchDepths[blob[1]] = true
		}
	}
	db.mu.RUnlock()
	if len(branchDepths) != config.ShardDepth {
		t.Fatalf("binary root should contain each prefix depth, got %v", branchDepths)
	}

	reloaded := NewTrie(root, db, hasher, config, true)
	for id, wantValue := range values {
		got, err := reloaded.Get(keys[id])
		if err != nil || !bytes.Equal(got, wantValue) {
			t.Fatalf("reloaded shard %d: got %q err %v, want %q", id, got, err, wantValue)
		}
	}
}

func TestZeroDepthUsesShardRootDirectly(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := NewPooledKeccakHasher()
	config := DefaultConfig()
	config.ShardDepth = 0
	trie := NewTrie(nil, db, hasher, config, true)
	key := bytes.Repeat([]byte{0x42}, 32)
	value := []byte("value")
	if err := trie.Put(key, value); err != nil {
		t.Fatal(err)
	}
	root, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(root, trie.GetShardRoot(0)) {
		t.Fatalf("zero-depth root should be the shard root: root=%x shard=%x", root, trie.GetShardRoot(0))
	}
	encoded, err := db.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 0 && encoded[0] == RootBranchHeader {
		t.Fatal("zero-depth trie wrote an unnecessary root branch")
	}
	reloaded := NewTrie(root, db, hasher, config, true)
	if got, err := reloaded.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("reloaded value: got %q err %v", got, err)
	}
}

func TestHashRunsDirtyShardsInParallel(t *testing.T) {
	db := NewMemoryDBAdapter()
	hasher := &concurrentHashTestHasher{inner: NewPooledKeccakHasher()}
	config := DefaultConfig()
	config.ShardDepth = 4
	config.CommitWorkers = 4
	trie := NewTrie(nil, db, hasher, config, true)
	for shard := 0; shard < 16; shard++ {
		key := make([]byte, 32)
		key[0] = byte(shard << 4)
		if err := trie.Put(key, []byte{byte(shard)}); err != nil {
			t.Fatal(err)
		}
	}
	hasher.enabled.Store(true)
	parallelRoot, err := trie.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if got := hasher.max.Load(); got < 2 {
		t.Fatalf("dirty shard hashes remained serial: max concurrency %d", got)
	}
	diag := LastHashDiagnostics()
	if diag.DirtyShards != 16 || diag.Workers != 4 {
		t.Fatalf("unexpected hash diagnostics: shards=%d workers=%d", diag.DirtyShards, diag.Workers)
	}

	hasher.enabled.Store(false)
	config.CommitWorkers = 1
	serialRoot, err := trie.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(parallelRoot, serialRoot) {
		t.Fatalf("parallel root changed result: parallel=%x serial=%x", parallelRoot, serialRoot)
	}
}

func TestCommitReusesHashesComputedByHash(t *testing.T) {
	hasher := &stemCountingHasher{inner: NewPooledKeccakHasher()}
	db := NewMemoryDBAdapter()
	config := DefaultConfig()
	config.ShardDepth = 0
	trie := NewTrie(nil, db, hasher, config, true)
	for i := 0; i < 32; i++ {
		key := make([]byte, 32)
		key[31] = byte(i)
		if err := trie.Put(key, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	root, err := trie.Hash()
	if err != nil {
		t.Fatal(err)
	}
	hasher.calls.Store(0)
	batch := db.NewBatch()
	committed, err := trie.CommitToBatch(batch, false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(root, committed) {
		t.Fatalf("commit changed precomputed root: hash=%x commit=%x", root, committed)
	}
	if calls := hasher.calls.Load(); calls != 0 {
		t.Fatalf("commit rehashed %d nodes after Hash", calls)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	// A later mutation must invalidate the cached path before the next root.
	key := make([]byte, 32)
	if err := trie.Put(key, []byte("changed")); err != nil {
		t.Fatal(err)
	}
	hasher.calls.Store(0)
	changedRoot, err := trie.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(root, changedRoot) || hasher.calls.Load() == 0 {
		t.Fatal("mutation reused a stale precomputed hash")
	}
}

func BenchmarkTrieHashWorkers(b *testing.B) {
	for _, workers := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			config := DefaultConfig()
			config.ShardDepth = 4
			config.CommitWorkers = workers
			trie := NewTrie(nil, NewMemoryDBAdapter(), NewPooledKeccakHasher(), config, true)
			keys := make([][]byte, 0, 16*512)
			for shard := 0; shard < 16; shard++ {
				for item := 0; item < 512; item++ {
					key := make([]byte, 32)
					key[0] = byte(shard << 4)
					binary.BigEndian.PutUint64(key[24:], uint64(item))
					keys = append(keys, key)
					if err := trie.Put(key, []byte{0}); err != nil {
						b.Fatal(err)
					}
				}
			}
			if _, err := trie.Hash(); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				for _, key := range keys {
					if err := trie.Put(key, []byte{byte(iteration + 1)}); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if _, err := trie.Hash(); err != nil {
					b.Fatal(err)
				}
			}
			diag := LastHashDiagnostics()
			b.ReportMetric(float64(diag.ShardWallNanos)/float64(time.Millisecond), "shard-wall-ms/op")
			b.ReportMetric(float64(diag.RootNanos)/float64(time.Millisecond), "root-merge-ms/op")
		})
	}
}

func BenchmarkTrieHashDepth20Workers(b *testing.B) {
	const (
		shardCount    = 1024
		itemsPerShard = 8
	)
	for _, workers := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			config := DefaultConfig()
			config.ShardDepth = 20
			config.CommitWorkers = workers
			trie := NewTrie(nil, NewMemoryDBAdapter(), NewPooledKeccakHasher(), config, true)
			keys := make([][]byte, 0, shardCount*itemsPerShard)
			for shard := 0; shard < shardCount; shard++ {
				// Spread the populated shard IDs evenly over the 20-bit space so
				// the benchmark also exercises the ordinary binary root paths.
				shardID := shard << 10
				for item := 0; item < itemsPerShard; item++ {
					key := make([]byte, 32)
					key[0] = byte(shardID >> 12)
					key[1] = byte(shardID >> 4)
					key[2] = byte(shardID << 4)
					binary.BigEndian.PutUint64(key[24:], uint64(item))
					keys = append(keys, key)
					if err := trie.Put(key, []byte{0}); err != nil {
						b.Fatal(err)
					}
				}
			}
			if _, err := trie.Hash(); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				for _, key := range keys {
					if err := trie.Put(key, []byte{byte(iteration + 1)}); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if _, err := trie.Hash(); err != nil {
					b.Fatal(err)
				}
			}
			diag := LastHashDiagnostics()
			b.ReportMetric(float64(diag.ShardWallNanos)/float64(time.Millisecond), "shard-wall-ms/op")
			b.ReportMetric(float64(diag.RootNanos)/float64(time.Millisecond), "root-merge-ms/op")
		})
	}
}
