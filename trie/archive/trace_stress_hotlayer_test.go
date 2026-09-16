// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.

package archive_test

// This file lives in the external test package because trie/archive/mpt
// imports trie/archive: the in-package trace stress test cannot import it
// without a cycle. The A6 hexary-MPT hot layer is registered through the
// archive.TraceHotTrieNew hook and selected with
// -traceStressHotLayer=mpt.

import (
	"errors"

	"github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/archive/mpt"
)

// mptHotLayer adapts the A6 MPT fallback trie to the trace stress hot-layer
// interface. Two translations are required:
//   - mpt.ErrNotFound becomes archive.ErrNodeNotFound, the sentinel the
//     workload treats as a key miss;
//   - CommitToBatch stages the trie's dirty nodes, archive records, flat
//     values and aggregate root into the driver's shared batch (T1
//     commit-merge, 2026-09-16). The driver flushes it with its own
//     DB_Write step; the trie no longer opens a private LevelDB batch per
//     commit. destructive is a no-op since shard tries are already
//     discarded on prune.
type mptHotLayer struct {
	trie *mpt.Trie
}

func (h mptHotLayer) Get(key []byte) ([]byte, error) {
	value, err := h.trie.Get(key)
	if errors.Is(err, mpt.ErrNotFound) {
		return nil, archive.ErrNodeNotFound
	}
	return value, err
}

func (h mptHotLayer) Put(key, value []byte) error { return h.trie.Put(key, value) }

func (h mptHotLayer) Delete(key []byte) error { return h.trie.Delete(key) }

func (h mptHotLayer) GetValueRef(key []byte) ([]byte, bool, error) {
	return h.trie.GetValueRef(key)
}

func (h mptHotLayer) PruneNextShard() error { return h.trie.PruneNextShard() }

func (h mptHotLayer) CommitToBatch(batch archive.Batcher, destructive bool) ([]byte, error) {
	return h.trie.CommitToBatch(batch, destructive)
}

func init() {
	archive.TraceHotTrieNew = func(kind string, db archive.KVStore, shardDepth, cuckooBuckets, cuckooSlots int, activateArchivedKeyOnRead bool) (archive.TraceHotTrie, error) {
		if kind != "mpt" {
			return nil, errors.New("trace hot layer hook: unsupported kind " + kind)
		}
		config := &mpt.Config{
			ShardDepth:                shardDepth,
			CuckooBuckets:             cuckooBuckets,
			CuckooSlots:               cuckooSlots,
			ActivateArchivedKeyOnRead: activateArchivedKeyOnRead,
		}
		trie, err := mpt.New(nil, db, config)
		if err != nil {
			return nil, err
		}
		return mptHotLayer{trie: trie}, nil
	}
}
