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

package mpt

import (
	"errors"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

// errUnsupported is returned by the parts of ethdb.KeyValueStore that the triedb
// layer never enters in this package. Every such method documents why it is
// unreachable; if pathdb ever grows a real dependency on one of them the
// dataset would silently degrade, so they return an error instead of pretending
// to succeed.
var errUnsupported = errors.New("mpt ethdb adapter: operation unsupported")

// pathTablePrefix isolates every key the triedb layer writes from the keys this
// package owns (MPTN nodes, MPTA archive records, MPTI index). All of them live
// in one LevelDB opened by the trace driver, so a collision would corrupt both.
const pathTablePrefix = "AMTP/"

// writeCounter tallies the bytes pathdb writes through the adapter.
//
// It exists because pathdb owns its own batching: none of its writes go through
// the caller's Batcher, so anything keyed off that batch reports zero for the
// path backend. Storage claims built on those numbers would silently understate
// the active layer, which is exactly what the G2 saving measurement reads.
type writeCounter struct {
	bytes  atomic.Int64
	writes atomic.Int64
	// totalBytes/totalWrites accumulate for the lifetime of the counter so
	// PathStats can report cumulative volume without disturbing the per-commit
	// attribution that take() drains.
	totalBytes  atomic.Int64
	totalWrites atomic.Int64
}

func (w *writeCounter) add(key, value []byte) {
	if w == nil {
		return
	}
	n := int64(len(key) + len(value))
	w.bytes.Add(n)
	w.writes.Add(1)
	w.totalBytes.Add(n)
	w.totalWrites.Add(1)
}

// takeRecent drains the counters so callers can attribute one commit's worth of
// volume. It returns cumulative written bytes and write count, and resets both.
func (w *writeCounter) take() (bytes int64, writes int64) {
	if w == nil {
		return 0, 0
	}
	return w.bytes.Swap(0), w.writes.Swap(0)
}

// totals reports lifetime-cumulative written bytes and write count without
// draining anything.
func (w *writeCounter) totals() (bytes int64, writes int64) {
	if w == nil {
		return 0, 0
	}
	return w.totalBytes.Load(), w.totalWrites.Load()
}

// kvStoreAdapter exposes an archive.KVStore as an ethdb.KeyValueStore so the
// triedb layer can back its trie nodes with the same database the archive
// driver already manages.
//
// Direction matters: every adapter already in the tree points the other way
// (ethdb -> KVStore), e.g. trie/archive/trie_test.go's LevelDBAdapter and
// stressDBAdapter, and this package's own testStore. This is the first one that
// has to go forward, because triedb.NewDatabase takes an ethdb.Database.
//
// The store lifecycle stays with the caller: Close is deliberately a no-op
// rather than a teardown, because the driver owns the LevelDB handle and closes
// it after this trie is gone.
type kvStoreAdapter struct {
	store   archivetrie.KVStore
	counter *writeCounter
}

func newKVStoreAdapter(store archivetrie.KVStore) ethdb.KeyValueStore {
	return &kvStoreAdapter{store: store}
}

func (a *kvStoreAdapter) Has(key []byte) (bool, error) {
	value, err := a.store.Get(key)
	if err != nil {
		if isMissingError(err) {
			return false, nil
		}
		return false, err
	}
	return value != nil, nil
}

func (a *kvStoreAdapter) Get(key []byte) ([]byte, error) {
	return a.store.Get(key)
}

func (a *kvStoreAdapter) Put(key, value []byte) error {
	return a.store.Put(key, value)
}

func (a *kvStoreAdapter) Delete(key []byte) error {
	return a.store.Delete(key)
}

func (a *kvStoreAdapter) NewBatch() ethdb.Batch {
	return &batchAdapter{Batcher: a.store.NewBatch(), counter: a.counter}
}

// NewBatchWithSize ignores its size hint because archive.Batcher exposes no
// allocation knob. pathdb pre-sizes its own write batch through this entry
// point (pathdb/buffer.go NewBatchWithSize); the returned batch is otherwise
// identical to NewBatch.
func (a *kvStoreAdapter) NewBatchWithSize(int) ethdb.Batch {
	return a.NewBatch()
}

// NewIterator yields an exhausted iterator carrying errUnsupported. pathdb only
// asks for real iterators inside its flat-state snapshot helpers
// (pathdb/iterator.go), which this package never reaches: there is no snapshot
// state to walk.
func (a *kvStoreAdapter) NewIterator(prefix, start []byte) ethdb.Iterator {
	return &failingIterator{err: errUnsupported}
}

func (a *kvStoreAdapter) Stat() (string, error) { return "", errUnsupported }

// DeleteRange is rendered unreachable by the same argument as NewIterator:
// pathdb issues range deletions only against ancient/frozen state, and the
// nofreezedb wrapper below reports no ancient datadir at all.
func (a *kvStoreAdapter) DeleteRange(start, end []byte) error { return errUnsupported }

func (a *kvStoreAdapter) Compact(start, limit []byte) error { return errUnsupported }

func (a *kvStoreAdapter) Close() error { return nil }

// batchAdapter lifts an archive.Batcher to ethdb.Batch. Put, Delete, Write,
// Reset and ValueSize already match; only Replay is missing. It also tallies
// volume into the shared counter, because these writes never pass through the
// caller's batch and would otherwise be invisible to storage accounting.
type batchAdapter struct {
	archivetrie.Batcher
	counter *writeCounter
}

func (b *batchAdapter) Put(key, value []byte) error {
	b.counter.add(key, value)
	return b.Batcher.Put(key, value)
}

func (b *batchAdapter) Delete(key []byte) error {
	b.counter.add(key, nil)
	return b.Batcher.Delete(key)
}

// Replay cannot be expressed through archive.Batcher: its queued entries are
// opaque to the caller once staged. Nothing in the triedb write path replays a
// batch, so this stays an explicit error rather than a silent no-op.
func (b *batchAdapter) Replay(ethdb.KeyValueWriter) error { return errUnsupported }

// failingIterator satisfies ethdb.Iterator without pretending to have data.
type failingIterator struct {
	err error
}

func (it *failingIterator) Next() bool    { return false }
func (it *failingIterator) Error() error  { return it.err }
func (it *failingIterator) Key() []byte   { return nil }
func (it *failingIterator) Value() []byte { return nil }
func (it *failingIterator) Release()      {}

// openPathDatabase builds a triedb.Database over the shared KVStore.
//
// StateHistory is pinned to 0: without that, pathdb tries to open a state-history
// freezer, and rawdb's nofreezedb has none. State history is what keeps journaled
// layers recoverable across restarts; this experiment never restarts mid-run
// (see the plan's "no checkpointing" rule), so skipping it is correct and also
// avoids thousands of needless freezer writes.
//
// The namespace prefix keeps triedb keys from interleaving with this package's
// own keys in the shared store.
//
// Two caller obligations were measured, not assumed, and both bite silently:
//
//  1. The initial lineage root is types.EmptyRootHash, NOT common.Hash{}.
//     Update and NodeReader both report "layer missing" for the zero hash.
//  2. Update's states argument must be non-nil — use triedb.NewStateSet().
//     pathdb dereferences it unguarded when building each diff layer
//     (pathdb/difflayer.go: states.size), so nil panics rather than erroring.
//
// openPathDatabase builds a triedb.Database over the shared KVStore and returns
// a counter that accumulates every byte it writes. Callers must drain the
// counter periodically, otherwise the number grows unbounded and stops being
// attributable to individual commits.
func openPathDatabase(store archivetrie.KVStore, cleanCacheBytes, writeBufferBytes int) (*triedb.Database, *writeCounter, error) {
	if store == nil {
		return nil, nil, errors.New("mpt ethdb adapter: nil store")
	}
	counter := &writeCounter{}
	disk := rawdb.NewDatabase(&kvStoreAdapter{store: store, counter: counter})
	namespaced := rawdb.NewTable(disk, pathTablePrefix)
	return triedb.NewDatabase(namespaced, &triedb.Config{
		PathDB: &pathdb.Config{
			StateHistory:    0,
			CleanCacheSize:  cleanCacheBytes,
			WriteBufferSize: writeBufferBytes,
		},
	}), counter, nil
}
