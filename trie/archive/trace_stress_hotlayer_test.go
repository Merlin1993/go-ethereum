// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package archive_test

// This file lives in the external test package because trie/archive/mpt
// imports trie/archive: the in-package trace stress test cannot import it
// without a cycle. The hexary-MPT hot layer is registered through the
// archive.TraceHotTrieNew hook and selected with -traceStressHotLayer=mpt.

import (
	"errors"

	"github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/archive/mpt"
)

// mptHotLayer adapts the MPT hot layer to the trace stress hot-layer interface.
// Two translations are required:
//   - mpt.ErrNotFound becomes archive.ErrNodeNotFound, the sentinel the
//     workload treats as a key miss;
//   - CommitToBatch stages archive records into the driver's shared batch and
//     hands dirty trie nodes to whichever backend is active. With the hash
//     backend those nodes also ride the shared batch; with the path backend they
//     go to pathdb instead, which is precisely why that backend exists.
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
	ref, fromArchive, err := h.trie.GetValueRef(key)
	if errors.Is(err, mpt.ErrNotFound) {
		return nil, false, archive.ErrNodeNotFound
	}
	return ref, fromArchive, err
}

func (h mptHotLayer) PruneNextShard() error { return h.trie.PruneNextShard() }

func (h mptHotLayer) CommitToBatch(batch archive.Batcher, destructive bool) ([]byte, error) {
	return h.trie.CommitToBatch(batch, destructive)
}

func (h mptHotLayer) Flush() error { return h.trie.Flush() }

// PathStats exposes the path-backend write accounting to the driver, which
// probes for it with a type assertion (same pattern as Flush).
func (h mptHotLayer) PathStats() (written, writes, buffered, diff int64) {
	return h.trie.PathStats()
}

func init() {
	archive.TraceMPTBucketCapacity = mpt.DefaultBucketCapacity
	archive.TraceHotTrieNew = func(spec archive.TraceHotTrieSpec) (archive.TraceHotTrie, error) {
		if spec.Kind != "mpt" {
			return nil, errors.New("trace hot layer hook: unsupported kind " + spec.Kind)
		}
		trie, err := mpt.New(nil, spec.DB, &mpt.Config{
			DomainNibbles:             spec.DomainNibbles,
			CuckooBuckets:             spec.CuckooBuckets,
			CuckooSlots:               spec.CuckooSlots,
			ActivateArchivedKeyOnRead: spec.ActivateArchivedKeyOnRead,
			Backend:                   spec.Backend,
			CleanCacheBytes:           spec.CleanCacheBytes,
			WriteBufferBytes:          spec.WriteBufferBytes,
			ForceCommitEveryBatches:   spec.FlushEveryBatches,
			ArchiveResidentEntries:    spec.ArchiveResidentEntries,
		})
		if err != nil {
			return nil, err
		}
		return mptHotLayer{trie: trie}, nil
	}
}
